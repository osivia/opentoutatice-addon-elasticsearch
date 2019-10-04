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
