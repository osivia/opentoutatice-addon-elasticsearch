# opentoutatice-addon-elasticsearch

## Zero down time re-indexing

### Objectif
Permettre à Nuxeo de ré-indexer un repository sur Es de façon transparente.

Actuellement, pour ré-indexer un repository, Nuxeo crée un nouvel index qu'il repeuple à partir de la BDD. 
La durée de ce traitement est longue.
En mode Full-Es, les utilisateurs doivent donc attendre la fin du traitement pour avoir à nouveau accès à l'ensemble des documents en lecture.
A noter que les opérations d'écriture sont correctement prises en compte (création, modification d'un document à nouveau accessible).

_Notes_:
* Mode Full-Es: réindexation synchrone d'au moins un document
* TODO: comment fonctionne la ré-indexation? lecture buckets en BDD mais si il y a une écriture, elle peut-être relue... Pas grave car minime

### Solution
Permettre à Nuxeo d'utiliser des alias (et pas directement des index).
Lors du processus de ré-indexation, on pourra donc:
*conserver l'accès aux documents en lecture en utilisant un alias en lecture pointant vers l'ancien index
*diriger les écritures vers le nouvel index (utilisation d'un alias en écriture pointant vers le nouvel index)
Cependant, les résultat des écritures doivent être accessibles: il faut donc également un alias en lecture sur le nouvel index. 

Conséquence des deux alias en lecture: à un moment donné, un document peut être présent dans les deux index; les requêtes peuvent donc retourner des doublons.
Il faudra donc pouvoir filtrer ces doublons en ne conservant que les documents présents dans le nouvel index.

Processus de ré-indexation = PR = (initialisation, ré-indexation, bascule)


##### Contraintes
1) le lancement ou appel du PR par un client HTTP ne doit pas attendre la fin du traitement pour rendre la main
 
   Corollaire:
   le lancement ou appel du PR doit rendre une réponse:
   * après l'étape 1 (si KO)
	 ou
   * après le lancement de l'étape 2 (lancement asynchrone de la ré-indexation)
 	
2) On ne doit pas pouvoir lancer un PR sur un repository donné si un PR est déjà en cours sur ce repository

3) On ne doit pas pouvoir lancer un PR sur un repository donné si un PR a déjà été effectué et que son status global est KO.

4) On doit pouvoir lancer en parallèle des PR s'ils sont sur des repository différents 


======================================================


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
