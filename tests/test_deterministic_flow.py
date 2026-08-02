#!/usr/bin/env python3
"""
Test standalone de la logique métier — sans Docker, sans Kafka réel, sans appel LLM.

Usage : python tests/test_deterministic_flow.py

Ce script ne dépend d'aucun fichier .env ni d'aucun service externe (Kafka,
MCP Confluent, providers LLM) : toutes les variables d'environnement
nécessaires sont fixées ci-dessous, et les fonctions testées sont soit pures
(is déterministes), soit appelées avec un producer Kafka factice
(FakeProducer) qui n'ouvre aucune connexion réseau.
"""

import json
import os
import sys
from pathlib import Path

# --- Variables d'environnement nécessaires (pas de dépendance à .env) ---
os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"
os.environ["MCP_CONFLUENT_URL"] = "http://localhost:3000"
os.environ["DIAGNOSTIC_LLM_PROVIDER"] = "openai"
os.environ["DIAGNOSTIC_LLM_MODEL"] = "gpt-4o"
os.environ["DIAGNOSTIC_LLM_API_KEY"] = ""
os.environ["REMEDIATION_LLM_PROVIDER"] = "anthropic"
os.environ["REMEDIATION_LLM_MODEL"] = "claude-sonnet-4-20250514"
os.environ["REMEDIATION_LLM_API_KEY"] = ""
os.environ["REPLAY_LLM_PROVIDER"] = ""
os.environ["REPLAY_LLM_MODEL"] = ""
os.environ["REPLAY_LLM_API_KEY"] = ""
os.environ["SHARE_GROUP_LOCK_DURATION_MS"] = "30000"
os.environ["SHARE_GROUP_MAX_DELIVERY_ATTEMPTS"] = "5"
os.environ["ENRICHMENT_SUCCESS_RATE"] = "0.8"

REPO_ROOT = Path(__file__).resolve().parent.parent
AGENTS_DIR = REPO_ROOT / "agents"
os.environ["SKILL_PATH"] = str(REPO_ROOT / "skills" / "kafka-streams-filter" / "SKILL.md")

# Les modules testés vivent sous agents/ (common, problem_injector, diagnostic,
# remediation, replay) et s'importent comme en production (PYTHONPATH=/app
# dans le Dockerfile).
sys.path.insert(0, str(AGENTS_DIR))

RESULTS: list[tuple[str, bool]] = []


def check(label: str, condition: bool) -> bool:
    """Affiche ✅/❌ pour une assertion individuelle."""
    print(f"  {'✅' if condition else '❌'} {label}")
    return condition


def run_test(name: str, func) -> None:
    print(f"\n=== {name} ===")
    try:
        ok = bool(func())
    except Exception as e:
        print(f"  ❌ Exception non gérée : {e!r}")
        ok = False
    RESULTS.append((name, ok))


# ---------------------------------------------------------------------------
# Test 1 — Imports
# ---------------------------------------------------------------------------

def test_imports() -> bool:
    modules = [
        "common.config",
        "common.share_group_client",
        "common.adk_factory",
        "problem_injector.app",
        "diagnostic.agent",
        "diagnostic.prompts",
        "remediation.agent",
        "remediation.prompts",
        "replay.agent",
        "replay.prompts",
    ]
    ok = True
    for mod_name in modules:
        try:
            __import__(mod_name)
            print(f"  ✅ import {mod_name}")
        except Exception as e:
            print(f"  ❌ import {mod_name} — {e}")
            ok = False
    return ok


# ---------------------------------------------------------------------------
# Test 2 — Config
# ---------------------------------------------------------------------------

def test_config() -> bool:
    try:
        from common import config
    except Exception as e:
        print(f"  ❌ Impossible d'importer common.config — {e}")
        return False

    ok = True
    for attr in (
        "DIAGNOSTIC_LLM_PROVIDER", "DIAGNOSTIC_LLM_MODEL", "DIAGNOSTIC_LLM_API_KEY",
        "REMEDIATION_LLM_PROVIDER", "REMEDIATION_LLM_MODEL", "REMEDIATION_LLM_API_KEY",
        "REPLAY_LLM_PROVIDER", "REPLAY_LLM_MODEL", "REPLAY_LLM_API_KEY",
        "TOPIC_FACTURATION", "TOPIC_FACTURATION_CORRIGE", "TOPIC_FACTURATION_DEAD_LETTER",
        "TOPIC_ALERTS", "TOPIC_INCIDENTS", "TOPIC_REPLAY_TASKS", "TOPIC_AUDIT",
    ):
        ok &= check(f"config.{attr} défini", hasattr(config, attr))

    print("\n  Config détectée :")
    print(f"    DIAGNOSTIC  : provider={config.DIAGNOSTIC_LLM_PROVIDER} model={config.DIAGNOSTIC_LLM_MODEL} "
          f"api_key={'(vide)' if not config.DIAGNOSTIC_LLM_API_KEY else '***'}")
    print(f"    REMEDIATION : provider={config.REMEDIATION_LLM_PROVIDER} model={config.REMEDIATION_LLM_MODEL} "
          f"api_key={'(vide)' if not config.REMEDIATION_LLM_API_KEY else '***'}")
    print(f"    REPLAY      : provider={config.REPLAY_LLM_PROVIDER or '(aucun)'} model={config.REPLAY_LLM_MODEL or '(aucun)'} "
          f"api_key={'(vide)' if not config.REPLAY_LLM_API_KEY else '***'}")
    print(f"    KAFKA_BOOTSTRAP_SERVERS={config.KAFKA_BOOTSTRAP_SERVERS}")
    print(f"    Topics: {config.TOPIC_FACTURATION}, {config.TOPIC_FACTURATION_CORRIGE}, "
          f"{config.TOPIC_ALERTS}, {config.TOPIC_INCIDENTS}, {config.TOPIC_REPLAY_TASKS}, {config.TOPIC_AUDIT}")

    return ok


# ---------------------------------------------------------------------------
# Test 3 — Prompts
# ---------------------------------------------------------------------------

def test_prompts() -> bool:
    try:
        from diagnostic.prompts import SYSTEM_PROMPT as DIAGNOSTIC_SYSTEM_PROMPT, DIAGNOSTIC_USER_PROMPT
        from remediation.prompts import SYSTEM_PROMPT as REMEDIATION_SYSTEM_PROMPT, REMEDIATION_USER_PROMPT
        from replay.prompts import SYSTEM_PROMPT as REPLAY_SYSTEM_PROMPT, REPLAY_USER_PROMPT
    except Exception as e:
        print(f"  ❌ Impossible d'importer les prompts — {e}")
        return False

    ok = True
    ok &= check("diagnostic.SYSTEM_PROMPT non vide", bool(DIAGNOSTIC_SYSTEM_PROMPT.strip()))
    ok &= check("remediation.SYSTEM_PROMPT non vide", bool(REMEDIATION_SYSTEM_PROMPT.strip()))
    ok &= check("replay.SYSTEM_PROMPT non vide", bool(REPLAY_SYSTEM_PROMPT.strip()))

    try:
        formatted = REMEDIATION_SYSTEM_PROMPT.format(skill_content="CONTENU SKILL DE TEST")
        ok &= check("remediation.SYSTEM_PROMPT.format(skill_content=...) se formatte", "CONTENU SKILL DE TEST" in formatted)
    except Exception as e:
        ok &= check(f"remediation.SYSTEM_PROMPT.format() a levé une exception — {e}", False)

    try:
        DIAGNOSTIC_USER_PROMPT.format(consumer_group="facturation", topic="facturation")
        ok &= check("DIAGNOSTIC_USER_PROMPT.format(...) se formatte", True)
    except Exception as e:
        ok &= check(f"DIAGNOSTIC_USER_PROMPT.format() a levé une exception — {e}", False)

    try:
        REMEDIATION_USER_PROMPT.format(
            incident_id="incident-1", consumer_group="facturation", cause="siret null",
            messages_affected=50, timestamp="2026-08-02T00:00:00Z",
        )
        ok &= check("REMEDIATION_USER_PROMPT.format(...) se formatte", True)
    except Exception as e:
        ok &= check(f"REMEDIATION_USER_PROMPT.format() a levé une exception — {e}", False)

    try:
        REPLAY_USER_PROMPT.format(message_json='{"id": 1, "siret": null}', attempt=1)
        ok &= check("REPLAY_USER_PROMPT.format(...) se formatte", True)
    except Exception as e:
        ok &= check(f"REPLAY_USER_PROMPT.format() a levé une exception — {e}", False)

    return ok


# ---------------------------------------------------------------------------
# Test 4 — Flow déterministe (Injection → Diagnostic → Remédiation → Replay)
# ---------------------------------------------------------------------------

class FakeProducer:
    """Producer Kafka factice : n'ouvre aucune connexion réseau, enregistre juste les appels."""

    def __init__(self):
        self.produced: list[dict] = []

    def produce(self, topic, key=None, value=None, on_delivery=None):
        self.produced.append({"topic": topic, "key": key, "value": value})
        if on_delivery:
            on_delivery(None, None)

    def flush(self, timeout=None):
        return 0


def test_deterministic_flow() -> bool:
    try:
        from problem_injector.app import make_message
        from diagnostic.agent import build_diagnostic
        from remediation.agent import generate_filter_code
        import replay.agent as replay_agent
    except Exception as e:
        print(f"  ❌ Impossible d'importer les fonctions du flow — {e}")
        return False

    ok = True

    # 1. Problem Injector — génération de messages valides/invalides
    valid_msg = make_message(1, valid=True)
    invalid_msg = make_message(2, valid=False)
    ok &= check("make_message(valid=True) a un siret", valid_msg.get("siret") is not None)
    ok &= check("make_message(valid=False) a siret=null", invalid_msg.get("siret") is None)

    # 2. Diagnostic — pattern-matching déterministe sur un échantillon
    sample_messages = [
        {"value": json.dumps(make_message(i, valid=(i <= 3)))} for i in range(1, 6)
    ]
    lag_data = {"state": "Stable"}
    incident = build_diagnostic("facturation", lag_data, sample_messages)
    ok &= check("build_diagnostic().cause == 'siret null'", incident.get("cause") == "siret null")
    ok &= check("build_diagnostic().messages_affected == 2", incident.get("messages_affected") == 2)
    ok &= check("build_diagnostic() a un incident_id", bool(incident.get("incident_id")))
    ok &= check("build_diagnostic().consumer_group == 'facturation'", incident.get("consumer_group") == "facturation")

    # 3. Remediation — génération déterministe du filtre Kafka Streams
    code = generate_filter_code(incident)
    ok &= check("generate_filter_code() retourne du code non vide", bool(code.strip()))
    ok &= check("generate_filter_code() référence le champ 'siret'", "siret" in code)
    ok &= check("generate_filter_code() référence le topic 'facturation'", "facturation" in code)

    # 4. Replay — enrichissement déterministe (random.random patché pour la reproductibilité)
    original_random = replay_agent.random.random
    try:
        replay_agent.random.random = lambda: 0.0  # toujours sous le seuil -> succès garanti
        enriched = replay_agent.deterministic_enrich({"id": 42, "siret": None, "montant": 10.0})
        ok &= check("deterministic_enrich() succès -> siret renseigné", enriched is not None and enriched.get("siret") is not None)

        replay_agent.random.random = lambda: 0.999  # toujours au-dessus du seuil -> échec garanti
        failed = replay_agent.deterministic_enrich({"id": 43, "siret": None, "montant": 10.0})
        ok &= check("deterministic_enrich() échec -> None", failed is None)
    finally:
        replay_agent.random.random = original_random

    return ok


# ---------------------------------------------------------------------------
# Test 5 — ADK factory (sans appel LLM)
# ---------------------------------------------------------------------------

def test_adk_factory() -> bool:
    try:
        from common.adk_factory import create_llm, AdkAgentRunner
    except Exception as e:
        print(f"  ❌ Impossible d'importer common.adk_factory — {e}")
        return False

    ok = True

    # create_llm() doit lever ValueError pour un provider inconnu, sans appeler de LLM
    try:
        create_llm("provider-bidon", "model-bidon", "fake-key")
        ok &= check("create_llm(provider inconnu) lève ValueError", False)
    except ValueError as e:
        ok &= check("create_llm(provider inconnu) lève ValueError('Unknown LLM provider')", "Unknown LLM provider" in str(e))
    except Exception as e:
        ok &= check(f"create_llm(provider inconnu) a levé {type(e).__name__} au lieu de ValueError — {e}", False)

    # AdkAgentRunner appelle create_llm() dès la construction (avant tout appel réseau),
    # donc l'erreur doit remonter à l'instanciation, sans jamais contacter de LLM.
    try:
        AdkAgentRunner(
            name="test-agent",
            description="agent de test",
            instruction="instruction de test",
            tools=[],
            provider="provider-bidon",
            model="model-bidon",
            api_key="fake-key",
        )
        ok &= check("AdkAgentRunner(provider inconnu) lève une erreur", False)
    except ValueError as e:
        ok &= check("AdkAgentRunner(provider inconnu) lève ValueError('Unknown LLM provider')", "Unknown LLM provider" in str(e))
    except Exception as e:
        ok &= check(f"AdkAgentRunner(provider inconnu) a levé {type(e).__name__} au lieu de ValueError — {e}", False)

    return ok


# ---------------------------------------------------------------------------
# Résultat final
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 70)
    print("TEST DÉTERMINISTE — flow métier sans Docker, sans Kafka, sans LLM")
    print("=" * 70)

    run_test("Test 1 — Imports", test_imports)
    run_test("Test 2 — Configuration", test_config)
    run_test("Test 3 — Prompts", test_prompts)
    run_test("Test 4 — Flow déterministe (Injection → Diagnostic → Remédiation → Replay)", test_deterministic_flow)
    run_test("Test 5 — ADK factory (sans appel LLM)", test_adk_factory)

    print("\n" + "=" * 70)
    print("RÉSUMÉ")
    print("=" * 70)
    passed = 0
    for name, ok in RESULTS:
        print(f"  {'✅' if ok else '❌'} {name}")
        passed += ok

    total = len(RESULTS)
    print(f"\nRésultat : {passed}/{total} tests passés")

    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
