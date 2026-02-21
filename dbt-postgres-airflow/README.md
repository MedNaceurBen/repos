# Real-World DBT Project from Scratch

## Description

Ce projet est un **pipeline de données complet** construit de A à Z pour démontrer mes compétences en **ingénierie de données et orchestration de workflows**. Il utilise **PostgreSQL, Airflow et DBT** pour extraire, transformer et charger des données depuis une API vers une base de données relationnelle, et créer des modèles de données prêts pour l'analyse.

## Technologies utilisées

* **DBT (Data Build Tool)** – modélisation et transformation des données
* **Apache Airflow** – orchestration des pipelines ETL
* **PostgreSQL** – stockage et gestion des données relationnelles
* **Docker** – pour isoler PostgreSQL et faciliter la configuration
* **Python** – appels API, traitement et automatisation
* **Homebrew & Penv** – gestion des dépendances et environnements Python

## Fonctionnalités principales

1. **Configuration de l’environnement**

   * Installation de Linux, Homebrew, Penv et création d’environnements Python isolés
   * Configuration de PostgreSQL via Docker

2. **Pipeline ETL automatisé**

   * Création de DAGs Airflow pour orchestrer l’extraction et l’insertion de données depuis une API
   * Automatisation de la récupération et du stockage des données

3. **Transformation et modélisation avec DBT**

   * Création de modèles DBT pour nettoyer, transformer et enrichir les données
   * Orchestration des modèles pour générer des tables finales fiables et analysables

4. **Automatisation et monitoring**

   * Planification automatique des pipelines avec Airflow
   * Suivi des transformations et vérification des résultats via DBT

## Structure du projet

```
project-root/
│
├─ airflow/                  # DAGs et scripts ETL
├─ dbt/                      # Modèles DBT et configurations
├─ postgres/                 # Configuration Docker PostgreSQL
├─ scripts/                  # Scripts Python pour API et transformations
├─ README.md                 # Documentation du projet
└─ requirements.txt          # Dépendances Python
```

## Résultat attendu

* Base PostgreSQL contenant des tables transformées et prêtes à l’analyse
* Pipeline ETL automatisé pour récupérer, transformer et charger les données
* Modèles DBT maintenables et extensibles pour analyses futures

## Instructions pour exécuter le projet

1. **Cloner le dépôt**

   ```bash
   git clone <URL_DU_PROJET>
   cd project-root
   ```

2. **Installer les dépendances Python**

   ```bash
   pip install -r requirements.txt
   ```

3. **Démarrer PostgreSQL avec Docker**

   ```bash
   docker-compose up -d
   ```

4. **Lancer Airflow**

   ```bash
   airflow standalone
   ```

5. **Exécuter les modèles DBT**

   ```bash
   dbt run
   ```

## Compétences démontrées

* Gestion d’environnements et installation d’outils (Python, Docker, PostgreSQL)
* Extraction et chargement de données via API
* Construction et orchestration de pipelines ETL avec Airflow
* Transformation et modélisation de données avec DBT
* Automatisation et planification de workflows
* Développement d’un projet de données de bout en bout prêt pour un usage réel
