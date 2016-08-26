[Installation]
l'installation se fait en décompressant l'archive tar dans le répertoire cible.

[Configuration]
La configuration se fait par le fichier client.properties
Celui-ci est commenté pour savoir ce qu'il est possible de configurer.
- Vérifier l'URL d'accés aux WebService du gestionnaire de données IKATS
  propriété : appUrl
- Vérifier le pattern des identifiants fonctionnels
  propriété : funcId.pattern
  !!Attention!!, si tag.aircraftId ou tag.flightId est positionné à NOT_SET, alors on ne peut pas les utiliser dans le FunctionalIdentifier
    Il ne sont pas ajouté en tant que Tag.
- Vérifier le mode de création des dataset :
  propriété : dataset.creation.mode : create, update ou none


La configuration du log se fait par le fichier log4j.xml.
il est possible de modifier le niveau des logs et de les rediriger vers un fichier



[pres requis d'execution]
Une JRE 1.7 minimum est requise pour pouvoir lancer 
la variable JAVA_HOME doit pointer sur cette JRE et la commande java par défaut doit être celle de cette JRE.
Si ce n'est pas le cas, modifier le script d'execution startup.sh pour pointer sur le bon executable java.

[Usage]
le lancement se fait par un interpréteur en ligne de commande en lancant le script startup.sh

Sans paramètre, le script affiche son 'Usage' :

Usage : startup.sh datasetName rootPath [logOnly]
--------------
  parameters :
 
  - [datasetName] : name of the imported dataset.
    all imported timeseries found in rootPath will be included into this dataset
 
  - [rootPath] : root directory from where to search Time Series csv files.
    Relative directory will be scanned to extract metadata and build metrics and tags.
    This directory must contains the datasetName subdirectory
 
  - [logOnly] (optional) : True or False, use to indicate a fake import,
    if True, logging all the file and metadata extracted from the path.
    if False, do the import without logging anything before.
    if ommited, information is logged and with a confirmation ([Enter]),
       import is done or cancel ([Ctrl-x]).
 
END 





