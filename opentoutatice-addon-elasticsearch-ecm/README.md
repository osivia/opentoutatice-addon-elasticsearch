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