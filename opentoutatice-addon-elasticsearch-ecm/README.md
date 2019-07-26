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
