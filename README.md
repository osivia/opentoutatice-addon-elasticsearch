## Utilisation

### Prérequis

Alias créé dans Es avec la convention de nommage _<nom>-alias_.

### Configuration

Période de vérification de fin de réindexation (en s):

ottc.reindexing.check.loop.period

(30 s par défaut)

### As Service

Le service n'est utilisable que par un 'Super Administarteur':

curl -X POST -H 'Content-Type: application/json+nxrequest' -u _super-admin-user_:_super-admin-passwd_ http://_nuxeo-host_:_nuxeo-port_/nuxeo/site/automation/Documents.ReIndexZeroDownTimeES -d {"params": {"repository":"default"}}







## Principe

[https://www.toutatice.fr/portail/share/Bj8Ods](https://www.toutatice.fr/portail/share/Bj8Ods)

*Fait*

## Gestion des erreurs

La réindexation se déroule en 3 étapes:
1. création et initialisation du nouvel index
2. réindexation
3. bascule sur le nouvel index

En cas d'erreur bloquante, la politique est la suivante:
1. suppression du nouvel index (ancien index utilisé)
2. et 3. on laisse le nouvel index en l'état (ancien index utilisé): une intervention manuelle devra être effectuée suite à la consultation des logs

*Fait*

## Gestion des doublons

Plutôt que d'effectuer un post-traitement sur le résultat des requêtes, l'idée est de filtrer les résultats au moment de la requête à l'aide de la notion d'aggrégats imbriqués de type bucket.
- les buckets de premier niveau sont nourris par la condition d'égalité des identifiants (ecm:uuid)
- les buckets de second niveau sont nourris par la condition d'appartenance au nouvel index

*En cours validation*

## Gestion des index

### Alias

Le principe de réindexation implique la nécessité de pouvoir gérer des index effectifs de noms différents de celui indiqué dans le nuxeo.conf.
Pour cela, l'idée est d'utiliser un alias plutôt qu'un index dans le nuxeo.conf.
Cet alias permet de basculer d'index en index de façon transparente sans avoir à gérer applicativement le nom réel des nouveaux index.

Note: cette mise en place nécessite:
- une convention de nommage pour l'alias dans le nuxeo.conf (alias_xxx) pour assurer l'unicité des noms d'alias/index lors de la première utilisation
- un fork de Nuxeo pour la prise en compte de cet alias au démarrage

*Validé*

### Purge

Suite à première réindexation, le cluster ES contient deux index et on choisit de conserver les deux:
l'alias actif (cf paragraphe précédent) pointe sur le nouvel index et un second alias pointe sur l'ancien (backup).
Pour toutes les réindexations suivantes, on purgera ES de sorte que seuls deux index subsistent.


## Quand réindexer?

### Propriétés statiques / dynamiques

### API _reindex
