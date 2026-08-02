# Génération d'un filtre Kafka Streams

**Compétence :** Génération d'un correctif Kafka Streams pour écarter ou corriger des messages fautifs
**Agent :** Agent de Remédiation (remediation-agent)
**Version :** 1.0

---

## Contexte

Cette procédure est déclenchée par l'agent de remédiation lorsqu'un incident est publié sur le topic `incidents` par l'agent de diagnostic. L'objectif est de générer le code d'une topologie Kafka Streams qui isole les messages fautifs du flux principal, sans bloquer le traitement des messages valides.

---

## Procédure en 6 étapes

### Étape 1 — Identifier le champ problématique et la condition de filtrage

À partir du message d'incident reçu sur le topic `incidents`, extraire :

- **`cause`** : la description de la cause racine (ex: `"siret null"`)
- **`messages_affected`** : le nombre de messages concernés
- **`consumer_group`** : le consumer group impacté (ex: `facturation`)

En déduire le **champ problématique** (ex: `siret`) et la **condition de filtrage** (ex: `siret is null`).

---

### Étape 2 — Générer une topologie Kafka Streams avec `filter()`

Construire une topologie qui lit le topic source, la sépare en deux branches avec `KStream#branch()` (ou `split()`/`branch()` selon la version) :

- **Branche valide** : le champ problématique est présent et non nul → republier sur `<topic>-valid` (ou laisser passer vers l'aval).
- **Branche rejetée** : tout le reste → router vers le dead-letter topic (étape 4).

---

### Étape 3 — Configurer le Serde approprié

Choisir le Serde selon le format des messages du topic source :

- **JSON** (cas par défaut de ce PoC) : `Serdes.String()` pour la clé, un `JsonSerde` custom (ou Jackson `ObjectMapper`) pour la valeur.
- **Avro** : `SpecificAvroSerde` avec Schema Registry.
- **String** : `Serdes.String()` des deux côtés, avec parsing manuel dans le predicate.

---

### Étape 4 — Ajouter la gestion d'erreurs (DLT)

Toute branche rejetée à l'étape 2 doit être publiée sur un **dead-letter topic** (`<topic>-dlt` ou `<topic>-dead-letter`) plutôt que d'être silencieusement supprimée, pour permettre une inspection ou un replay ultérieur (voir l'agent de Replay, KIP-932).

Envelopper les opérations `filter`/`branch` dans une gestion d'exception (`DeserializationExceptionHandler` côté config du `StreamsBuilder`) pour que les messages non désérialisables (JSON malformé) suivent aussi le chemin dead-letter au lieu de crasher le stream.

---

### Étape 5 — Générer le docker-compose de test local

Fournir un extrait `docker-compose.test.yml` minimal permettant de valider la topologie en local : un broker Kafka KRaft mono-nœud, le topic source pré-créé, et le job Kafka Streams packagé en conteneur, pointant sur `APPLICATION_ID_CONFIG` dédié pour ne pas entrer en conflit avec la stack principale.

---

### Étape 6 — Valider avec un test unitaire sur des messages synthétiques

Utiliser `TopologyTestDriver` (module `kafka-streams-test-utils`) pour valider le comportement sans broker réel :

1. Injecter un message **valide** (champ problématique renseigné) → vérifier qu'il atterrit sur la branche valide.
2. Injecter un message **invalide** (champ problématique nul/absent) → vérifier qu'il atterrit sur le dead-letter topic.
3. Injecter un message **malformé** (JSON invalide) → vérifier qu'il est capté par le `DeserializationExceptionHandler` et routé vers le dead-letter topic sans faire planter le driver.

---

## Règles complémentaires

- **Ne jamais supprimer silencieusement un message** : tout message écarté doit être traçable via le dead-letter topic ou le topic `audit`.
- **Idempotence** : le filtre ne doit avoir aucun effet de bord sur les messages valides — ils doivent transiter inchangés.
- **Traçabilité** : le code généré et son déploiement doivent être journalisés dans le topic `audit` (identifiant d'incident, code généré, horodatage).
