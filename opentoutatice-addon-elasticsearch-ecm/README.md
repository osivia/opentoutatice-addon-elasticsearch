# opentoutatice-addon-elasticsearch

#### Surveillance des requêtes Es volumineuses appelées par automation
Par défaut, une requête est dite volumineuse si elle retourne plus de 1000 résultats.

Cette valeur peut être modifiée dans le <b>nuxeo.conf</b> via la propriété <b>ottc.es.query.default.limit</b>.

Il y a trois UC pour lesquels cette valeur peut-être dépassée:
- la requête est explicitement non paginée: limit = -1 (Es interprète le -1 envalorisant la limite à 2147483647)
- la requête n'indique aucun critère de pagination et utilise alors l'ancienne limite par défaut: 10000
- la requête est explicitement paginée avec une limite supérieure à la valeur par défaut

Les requêtes de plus de 1000 résultats peuvent être tracées dans le fichier $NUXEO_LOG_DIR/automation-es-bulky-queries-monitoring.log à l'aide de la configuration suivante de log4j (testée avec la version 1):

```  
<appender name="ES-BULKY-QUERIES" class="org.apache.log4j.DailyRollingFileAppender">
    <errorHandler class="org.apache.log4j.helpers.OnlyOnceErrorHandler" />
    <param name="File" value="${nuxeo.log.dir}/automation-es-bulky-queries-monitoring.log" />
    <param name="Append" value="true" />
    <!-- Rollover at midnight every day -->
    <param name="DatePattern" value="'.'yyyy-MM-dd" />
    <layout class="org.apache.log4j.PatternLayout">
      <param name="ConversionPattern" value="%d{ISO8601} %-5p [%t] [%c] %m%n" />
    </layout>
  </appender>
  
  <category name="fr.toutatice.ecm.elasticsearch.automation.QueryES" additivity="false">
    <priority value="INFO" />
    <appender-ref ref="ES-BULKY-QUERIES"/>
  </category>
```

<b><i>Note:</i></b>

- les requêtes non paginées appelées par Nuxeo core (i.e. pas par un appel REST) nes ont pas tracées
- l'addon offre la possibilité d'effectuer des requêtes directement en ES (passthrough sans NXQL).Ces requêtes ne sont également pas tracées.

#### TODO
##### Paginer une requête non bornée
Actuellement, si une requête est explicitement non paginée (-1), on interroge malgré to Es sans pagination (limite à 2147483647).
Dans ce cas, il faudra paginer la requête, i.e. remplacer -1 par 1000.
##### Optimiser l'utilisation des patterns regex

- `TTCEsCodec`: l.93
- `TTCSearchResponse`: l.77

## Recherche Fulltext

### Prérequis
Installer sur au moins un noeud du cluster Es le plugin ICU (tokenizer):

`cd /usr/share/elasticsearch`

`./bin/plugin install elasticsearch/elasticsearch-analysis-icu/2.7.0`

Le redémarrage des noeuds n'est pas nécessaire.

### Configuration

La configuration des champs full-text s'effectue dans le fichier `/opt/nuxeo/templates/<template>/nxserver/config/<template>-elasticsearch-config.xml.nxftl`

#### Définition d'un champ full-text

Pour le champ `pf:example`:

 ```
 "pf:example" : {
   	"type" : "string",
    "analyzer" : "fulltext"          		
  },
```

Ajouter ensuite ce champ à la liste des champs full-text:

```
<fullTextFields>
	<field>pf:example</field>
	...
</fullTextFields>
```
#### Note: champ à valeur exacte et full-text

Un champ peut-être à la fois considéré comme full-text (analysé 'full-text' à l'indexation) et à valeur exacte (non analysé à l'indexation - écrit tel quel).

Par exemple, le champ `dc:title` est considéré comme à valeur exacte dans les requêtes Nuxeo de type `LIKE`.

On doit donc définir le champ `dc:title` avec 2 méta-champs: 1 à valeur exacte et 1 full-text:

```
"dc:title" : {
         "type" : "multi_field",
         "fields" : {
           "dc:title" : {
	           	"type" : "string",
				"index": "not_analyzed"
           },
           "fulltext" : {
	             "type": "string",
	             "analyzer" : "fulltext"
          }
        }
      },
```

De plus, pour les requêtes de type `ILIKE`, le champs doit aussi être indexé en minuscules. la définition de `dc:title` comprendra donc 3 méta-champs:

```
"dc:title" : {
         "type" : "multi_field",
         "fields" : {
           "dc:title" : {
	           	"type" : "string",
				"index": "not_analyzed"
           },
           "fulltext" : {
	             "type": "string",
	             "analyzer" : "fulltext"
          },
          "lowercase" : {
	            "type":"string",
	            "analyzer" : "lowercase_analyzer"       
          }
        }
      },
```

#### Highlight

Les extraits retournés sont configurables via le fichier nuxeo.conf.

Par défaut:

`ottc.fulltext.query.highlight.pre.tag=<span class="highlight">`

`ottc.fulltext.query.highlight.pre.tag=</span>`     :    tags encadrant les termes trouvés dans les xtraits

`ottc.fulltext.query.highlight.fragments.size=100`  :    taille de chaque extrait

`ottc.fulltext.query.highlight.fragments.number=5`  :    nombre maximum d'extraites retournés par champ

### Principe

Le fichier `/opt/nuxeo/templates/<template>/nxserver/config/<template>-elasticsearch-config.xml.nxftl` contient la liste explicite des champs full-text requêtables (`<fullTextFields>...</fullTextFields>`). Cette liste indique également les champs dont des extraits peuvent être retournés par le highlight.

Si la liste `<fullTextFields>...</fullTextFields>` n'est pas renseignée, les champs `"dc:title.fulltext^2", "dc:description^1.5", "note:note", "ecm:binarytext"` sont utilisés.

La notation `^x` indique le boost appliqué au champ.

Il est cependant possible de court-circuiter cette liste par défaut ou la liste configurée en indiquant explicitement les champs requêtés par l'intermédiaire du paramètre `fields` de l'endpoint `search` du webservice `ContentStore`.
Ce paramètre accepte des chaînes de caractères séparées par des virgules comme:

`"fields": "dc:description^1.5, annonce:resume, note:note"`

##### Toutatice-apps

Pour toutatice-apps, la liste des champs full-ext est:

```
<fullTextFields>
      	<field>dc:title.fulltext^2</field>
      	<field>dc:description^1.5</field>
      	<field>annonce:resume</field>
      	<field>note:note</field>
        <field>webp:content</field>
        <field>webc:welcomeText</field>
        <field>comment:comment</field>     
        <field>ttcth:message</field>
        <field>unum:scenario</field>
        <field>mail:mail</field>
      	<field>ecm:binarytext</field>
      </fullTextFields>
```

Elle permet de couvrir les Espace et pages de publication, les WebSites, Blogs, Forums, Fils de discussion, Articles, Notes, Commentaires, File, Usages Numériques et mails.




