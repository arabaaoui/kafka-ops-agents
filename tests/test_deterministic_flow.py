#!/usr/bin/env python3
"""
Test standalone de la logique métier — sans Docker, sans Kafka réel, sans appel LLM.

Usage : python tests/test_deterministic_flow.py

Ce script ne dépend d'aucun fichier .env ni d'aucun service externe (Kafka,
MCP Confluent, providers LLM) : toutes les variables d'environnement
nécessaires sont fixées ci-dessous, et les fonctions testées sont soit pures
(déterministes), soit appelées avec un producer Kafka factice (FakeProducer)
qui n'ouvre aucune connexion réseau.
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
os.environ["SHARE_GROUP_LOCK_DURATION_MS"] = "30000"
os.environ["SHARE_GROUP_DELIVERY_COUNT_LIMIT"] = "5"

REPO_ROOT = Path(__file__).resolve().parent.parent
AGENTS_DIR = REPO_ROOT / "agents"

# Les modules testés vivent sous agents/ (common, problem_injector, diagnostic,
# remediation) et s'importent comme en production (PYTHONPATH=/app dans le
# Dockerfile).
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
        "common.simulated_control_plane",
        "problem_injector.app",
        "diagnostic.agent",
        "diagnostic.prompts",
        "remediation.agent",
        "remediation.prompts",
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
        "TOPIC_FACTURES", "TOPIC_ALERTS", "TOPIC_INCIDENTS",
        "TOPIC_REMEDIATION_CONFIRMATIONS", "TOPIC_AUDIT",
        "FACTURATION_CONSUMER_GROUP", "TARGET_LAG",
    ):
        ok &= check(f"config.{attr} défini", hasattr(config, attr))

    print("\n  Config détectée :")
    print(f"    DIAGNOSTIC  : provider={config.DIAGNOSTIC_LLM_PROVIDER} model={config.DIAGNOSTIC_LLM_MODEL} "
          f"api_key={'(vide)' if not config.DIAGNOSTIC_LLM_API_KEY else '***'}")
    print(f"    REMEDIATION : provider={config.REMEDIATION_LLM_PROVIDER} model={config.REMEDIATION_LLM_MODEL} "
          f"api_key={'(vide)' if not config.REMEDIATION_LLM_API_KEY else '***'}")
    print(f"    KAFKA_BOOTSTRAP_SERVERS={config.KAFKA_BOOTSTRAP_SERVERS}")
    print(f"    Topics: {config.TOPIC_FACTURES}, {config.TOPIC_ALERTS}, {config.TOPIC_INCIDENTS}, "
          f"{config.TOPIC_REMEDIATION_CONFIRMATIONS}, {config.TOPIC_AUDIT}")
    print(f"    TARGET_LAG={config.TARGET_LAG}")

    return ok


# ---------------------------------------------------------------------------
# Test 3 — Prompts
# ---------------------------------------------------------------------------

def test_prompts() -> bool:
    try:
        from diagnostic.prompts import SYSTEM_PROMPT as DIAGNOSTIC_SYSTEM_PROMPT, DIAGNOSTIC_USER_PROMPT
        from remediation.prompts import (
            SYSTEM_PROMPT as REMEDIATION_SYSTEM_PROMPT,
            REMEDIATION_PROPOSE_PROMPT,
            REMEDIATION_EXECUTE_PROMPT,
        )
    except Exception as e:
        print(f"  ❌ Impossible d'importer les prompts — {e}")
        return False

    ok = True
    ok &= check("diagnostic.SYSTEM_PROMPT non vide", bool(DIAGNOSTIC_SYSTEM_PROMPT.strip()))
    ok &= check("remediation.SYSTEM_PROMPT non vide", bool(REMEDIATION_SYSTEM_PROMPT.strip()))

    try:
        DIAGNOSTIC_USER_PROMPT.format(consumer_group="facturation", topic="factures")
        ok &= check("DIAGNOSTIC_USER_PROMPT.format(...) se formatte", True)
    except Exception as e:
        ok &= check(f"DIAGNOSTIC_USER_PROMPT.format() a levé une exception — {e}", False)

    try:
        REMEDIATION_PROPOSE_PROMPT.format(
            incident_id="incident-1", consumer_group="facturation", cause="compression.type=producer",
            topic="factures", recommended_config='{"compression.type": "lz4"}',
            config_key="compression.type", current_value="producer", new_value="lz4",
            timestamp="2026-08-20T00:00:00Z",
        )
        ok &= check("REMEDIATION_PROPOSE_PROMPT.format(...) se formatte", True)
    except Exception as e:
        ok &= check(f"REMEDIATION_PROPOSE_PROMPT.format() a levé une exception — {e}", False)

    try:
        REMEDIATION_EXECUTE_PROMPT.format(
            incident_id="incident-1", topic="factures", config_key="compression.type", new_value="lz4",
        )
        ok &= check("REMEDIATION_EXECUTE_PROMPT.format(...) se formatte", True)
    except Exception as e:
        ok &= check(f"REMEDIATION_EXECUTE_PROMPT.format() a levé une exception — {e}", False)

    return ok


# ---------------------------------------------------------------------------
# Test 4 — Flow déterministe (Injection → Diagnostic → Proposition → Exécution)
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
        from remediation.agent import RemediationTools
        from common.simulated_control_plane import get_topic_config_simulated, alter_topic_config_simulated
    except Exception as e:
        print(f"  ❌ Impossible d'importer les fonctions du flow — {e}")
        return False

    ok = True

    # 1. Problem Injector — génération de messages de facturation (pas de champ défectueux)
    msg = make_message(1)
    ok &= check("make_message() a un id", msg.get("id") == 1)
    ok &= check("make_message() a un montant", isinstance(msg.get("montant"), float))
    ok &= check("make_message() a un client_id", bool(msg.get("client_id")))

    # 2. Diagnostic — la cause vient de la configuration du topic, pas du contenu
    lag_data = {"state": "Stable"}
    incident_bad_config = build_diagnostic("facturation", lag_data, {"compression.type": "producer"})
    ok &= check(
        "build_diagnostic() detecte compression.type=producer",
        "retaille" in incident_bad_config.get("cause", ""),
    )
    ok &= check(
        "build_diagnostic() recommande lz4",
        incident_bad_config.get("recommended_config") == {"compression.type": "lz4"},
    )
    ok &= check("build_diagnostic() a un incident_id", bool(incident_bad_config.get("incident_id")))

    incident_ok_config = build_diagnostic("facturation", lag_data, {"compression.type": "lz4"})
    ok &= check(
        "build_diagnostic() ne propose rien quand la config est déjà correcte",
        incident_ok_config.get("recommended_config") == {},
    )

    # 3. Simulated control plane — get/alter topic config, aucun appel réseau
    current = get_topic_config_simulated("factures")
    ok &= check("get_topic_config_simulated() renvoie compression.type=producer", current.get("compression.type") == "producer")

    alter_topic_config_simulated("factures", "compression.type", "lz4")
    updated = get_topic_config_simulated("factures")
    ok &= check("alter_topic_config_simulated() applique le changement", updated.get("compression.type") == "lz4")

    # 4. Remediation — porte de confirmation appliquée côté code
    producer = FakeProducer()
    tools = RemediationTools(producer)

    tools.propose_change("factures", "compression.type", "producer", "lz4", incident_id="incident-1")
    ok &= check("propose_change() enregistre une proposition en attente", "incident-1" in tools.pending)
    ok &= check(
        "propose_change() publie un audit pending_confirmation",
        any(json.loads(p["value"]).get("status") == "pending_confirmation" for p in producer.produced),
    )

    refused = tools.execute_change("factures", "compression.type", "lz4", incident_id="incident-inconnu")
    ok &= check("execute_change() refuse sans proposition en attente", refused.startswith("REFUS"))

    applied = tools.execute_change("factures", "compression.type", "lz4", incident_id="incident-1")
    ok &= check("execute_change() applique la proposition confirmée", applied.startswith("OK"))
    ok &= check("execute_change() retire la proposition traitée", "incident-1" not in tools.pending)
    ok &= check(
        "execute_change() publie un audit simulated_applied",
        any(json.loads(p["value"]).get("status") == "simulated_applied" for p in producer.produced),
    )

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
    run_test("Test 4 — Flow déterministe (Injection → Diagnostic → Proposition → Exécution)", test_deterministic_flow)
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
