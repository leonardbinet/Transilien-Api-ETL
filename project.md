# TODO

## Extraction
Un numéro de train peut il passer par la même station le même jour?
On ne dirait pas.
```
import pandas as pd
df_flat = pd.read_csv("data/gtfs-lines-last/flat.csv")
group_df = df_flat.groupby(["trip_id","station_id"]).count()
(group_df.departure_time != 1).sum()
(group_df.departure_time == 1).sum()
```

Que se passe t-il après minuit??
Le train a t-il deux jour de suite le même numéro? Est ce que l'on va écraser les infos?

Au pire on n'a plus les infos après minuit, et le train du jour suivant va écraser les stations. => pas dramatique.


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


## Documentation:

### Ressources utiles

API TRANSILIEN
https://ressources.data.sncf.com/explore/dataset/api-temps-reel-transilien/

HORAIRES DES LIGNES
https://ressources.data.sncf.com/explore/dataset/sncf-transilien-gtfs/information/

BLOG INTERESSANT
https://x0r.fr/blog/23
https://bitbucket.org/xtab/rer-web/src
