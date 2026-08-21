#!/usr/bin/env python3
"""
Test standalone de la logique métier — sans Docker, sans Kafka réel, sans appel LLM.

Usage : python tests/test_deterministic_flow.py

Ce script ne dépend d'aucun fichier .env ni d'aucun service externe (Kafka,
MCP Confluent, provider LLM) : toutes les variables d'environnement
nécessaires sont fixées ci-dessous, et les fonctions testées sont soit pures
(déterministes), soit appelées avec un producer Kafka factice (FakeProducer)
qui n'ouvre aucune connexion réseau. apply_fix_simulated()/verify_fix() ne
sont testés que sur leur porte de refus (pas de diagnostic en attente),
seul chemin qui n'ouvre pas de connexion Kafka réelle.
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

REPO_ROOT = Path(__file__).resolve().parent.parent
AGENTS_DIR = REPO_ROOT / "agents"

# Les modules testés vivent sous agents/ (common, problem_injector, diagnostic)
# et s'importent comme en production (PYTHONPATH=/app dans le Dockerfile).
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
        "common.adk_factory",
        "common.mcp_client",
        "problem_injector.app",
        "diagnostic.agent",
        "diagnostic.prompts",
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
        "KAFKA_BOOTSTRAP_SERVERS", "MCP_CONFLUENT_URL",
        "TOPIC_FACTURES", "TOPIC_ALERTS", "TOPIC_INCIDENTS",
        "FACTURATION_CONSUMER_GROUP", "FACTURATION_PARTITION", "POISON_OFFSET",
        "MESSAGES_AFTER_POISON", "DIAGNOSTIC_SCAN_COUNT",
        "VERIFY_FIX_DELAY_S", "CATCHUP_TIMEOUT_S",
    ):
        ok &= check(f"config.{attr} défini", hasattr(config, attr))

    print("\n  Config détectée :")
    print(f"    DIAGNOSTIC : provider={config.DIAGNOSTIC_LLM_PROVIDER} model={config.DIAGNOSTIC_LLM_MODEL} "
          f"api_key={'(vide)' if not config.DIAGNOSTIC_LLM_API_KEY else '***'}")
    print(f"    KAFKA_BOOTSTRAP_SERVERS={config.KAFKA_BOOTSTRAP_SERVERS}")
    print(f"    MCP_CONFLUENT_URL={config.MCP_CONFLUENT_URL}")
    print(f"    Topics: {config.TOPIC_FACTURES}, {config.TOPIC_ALERTS}, {config.TOPIC_INCIDENTS}")
    print(f"    FACTURATION_CONSUMER_GROUP={config.FACTURATION_CONSUMER_GROUP} "
          f"partition={config.FACTURATION_PARTITION} POISON_OFFSET={config.POISON_OFFSET}")

    return ok


# ---------------------------------------------------------------------------
# Test 3 — Prompts
# ---------------------------------------------------------------------------

def test_prompts() -> bool:
    try:
        from diagnostic.prompts import SYSTEM_PROMPT, DIAGNOSTIC_USER_PROMPT
    except Exception as e:
        print(f"  ❌ Impossible d'importer les prompts — {e}")
        return False

    ok = True
    ok &= check("diagnostic.SYSTEM_PROMPT non vide", bool(SYSTEM_PROMPT.strip()))

    try:
        DIAGNOSTIC_USER_PROMPT.format(consumer_group="facturation", topic="factures", partition=0)
        ok &= check("DIAGNOSTIC_USER_PROMPT.format(...) se formatte", True)
    except Exception as e:
        ok &= check(f"DIAGNOSTIC_USER_PROMPT.format() a levé une exception — {e}", False)

    return ok


# ---------------------------------------------------------------------------
# Test 4 — Flow déterministe (Injection → Diagnostic → Fix simulé → Vérification)
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
        from problem_injector.app import make_message, make_poison_message
        from diagnostic.agent import build_diagnostic, DiagnosticTools, _is_broken
        from common.mcp_client import extract_partition_lag
    except Exception as e:
        print(f"  ❌ Impossible d'importer les fonctions du flow — {e}")
        return False

    ok = True

    # 1. Problem Injector — messages valides vs. poison message
    msg = make_message(1)
    ok &= check("make_message() a un siret", bool(msg.get("siret")))
    ok &= check("make_message() a un montant", isinstance(msg.get("montant"), float))
    ok &= check("make_message() a un client_id", bool(msg.get("client_id")))

    poison = make_poison_message(42)
    ok &= check("make_poison_message() n'a pas de siret", "siret" not in poison)

    # 2. _is_broken() — détection du message fautif
    valid_wire = {"value": json.dumps(make_message(2))}
    poison_wire = {"value": json.dumps(make_poison_message(2))}
    malformed_wire = {"value": "not valid json"}
    ok &= check("_is_broken() renvoie False pour un message valide", _is_broken(valid_wire) is False)
    ok &= check("_is_broken() renvoie True pour le poison message (siret absent)", _is_broken(poison_wire) is True)
    ok &= check("_is_broken() renvoie True pour un JSON invalide", _is_broken(malformed_wire) is True)

    # 3. mcp_client.extract_partition_lag() — extraction pure d'une ligne de partition
    lag_payload = {"topics": [{"topic": "factures", "partitions": [
        {"partition": 0, "committedOffset": 1452, "highWatermark": 1483, "lag": 31}
    ]}]}
    partition_lag = extract_partition_lag(lag_payload, "factures", 0)
    ok &= check("extract_partition_lag() retrouve la bonne partition", partition_lag.get("committedOffset") == 1452)

    # 4. build_diagnostic() — cause racine + commande CLI proposée
    lag_data = {**partition_lag, "stagnant": True}
    incident = build_diagnostic("facturation", lag_data, [poison_wire], [valid_wire])
    ok &= check("build_diagnostic() identifie la cause siret absent", "siret" in incident.get("cause", ""))
    ok &= check("build_diagnostic() ne détecte pas de rafale (scan propre)", incident.get("burst") is False)
    ok &= check(
        "build_diagnostic() propose la commande --reset-offsets",
        "--reset-offsets" in incident.get("reset_command", "") and "--to-offset 1453" in incident["reset_command"],
    )
    ok &= check("build_diagnostic() a une précondition (arrêter le consumer)", "Arrêter" in incident.get("precondition", ""))
    ok &= check("build_diagnostic() a un incident_id", bool(incident.get("incident_id")))

    incident_burst = build_diagnostic("facturation", lag_data, [poison_wire], [poison_wire])
    ok &= check("build_diagnostic() détecte une rafale quand le scan contient aussi des messages cassés", incident_burst.get("burst") is True)

    # 5. DiagnosticTools — porte de refus appliquée côté code (pas seulement dans le prompt)
    producer = FakeProducer()
    tools = DiagnosticTools(producer, admin=None)

    refused_fix = tools.apply_fix_simulated()
    ok &= check("apply_fix_simulated() refuse sans diagnostic préalable", refused_fix.startswith("REFUS"))
    refused_verify = tools.verify_fix()
    ok &= check("verify_fix() refuse sans diagnostic préalable", refused_verify.startswith("REFUS"))

    result = tools.diagnose(json.dumps(lag_data), json.dumps([poison_wire]), json.dumps([valid_wire]))
    ok &= check("diagnose() confirme la publication", result.startswith("OK"))
    ok &= check("diagnose() marque incident_produced", tools.incident_produced is True)
    ok &= check(
        "diagnose() publie l'incident sur 'incidents'",
        any(json.loads(p["value"]).get("event") == "diagnostic" for p in producer.produced),
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
    run_test("Test 4 — Flow déterministe (Injection → Diagnostic → Fix simulé → Vérification)", test_deterministic_flow)
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
