# TODO

## Setup
Create Virtualenv:

```
conda create -n api_transilien python=3
# to activate it
source activate api_transilien
pip install -r requirements.txt
```

## Extraire données
Pour lancer le script: (par défault, cycle de 1200 secondes:20 minutes)
```
python main.py extract
```
Sinon pour choisir votre temps de cycle: celui ci fera un cycle de 2 minutes (120 secondes).
```
python main.py extract 120
```

### Choisir les lignes à suivre
Liste de lignes que l'on veut suivre

=> Liste de gares que l'on veut suivre

=> Les regarder toutes les minutes

### Choisir 'tempo' et granularité
Réflechir à quel rythme choisir, et voir s'il est nécessaire de regarder tous les arrêts


## Relier données:

- Trouver données des plannings.
- Savoir relier plannings des trains à leur horaires réels

## Faire un prototype de prédiction simple
