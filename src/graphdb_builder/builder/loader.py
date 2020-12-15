"""
    Populates the graph database with all the files generated by the importer.py module:
    Ontologies, Databases and Experiments.
    The module loads all the entities and relationships defined in the importer files. It
    calls Cypher queries defined in the cypher.py module. Further, it generates an hdf object
    with the number of enities and relationships loaded for each Database, Ontology and Experiment.
    This module also generates a compressed backup file of all the loaded files.

    There are two types of updates:

    - Full: all the entities and relationships in the graph database are populated
    - Partial: only the specified entities and relationships are loaded

    The compressed files for each type of update are named accordingly and saved in the archive/ folder
    in data/.
"""

import os
import sys
import re
from datetime import datetime
import config.ckg_config as ckg_config
import ckg_utils
from graphdb_connector import connector
from graphdb_builder import builder_utils


cwd = os.path.abspath(os.path.dirname(__file__))
log_config = ckg_config.graphdb_builder_log
logger = builder_utils.setup_logging(log_config, key="loader")
START_TIME = datetime.now()

try:
    config = builder_utils.setup_config('builder')
    directories = builder_utils.get_full_path_directories()
except Exception as err:
    logger.error("Reading configuration > {}.".format(err))


def load_into_database(driver, queries, requester):
    """
    This function runs the queries provided in the graph database using a neo4j driver.

    :param driver: neo4j driver, which provides the connection to the neo4j graph database.
    :type driver: neo4j driver
    :param list[dict] queries: list of queries to be passed to the database.
    :param str requester: identifier of the query.
    """
    regex = r"file:\/\/\/(.+\.tsv)"
    result = None
    for query in queries:
        try:
            if "file" in query:
                matches = re.search(regex, query)
                if matches:
                    file_path = matches.group(1)
                    if os.path.isfile(file_path):
                        result = connector.commitQuery(driver, query+";")
                        record = result.single()
                        if record is not None and 'c' in record:
                            counts = record['c']
                            if counts == 0:
                                logger.warning("{} - No data was inserted in query: {}.\n results: {}".format(requester, query, counts))
                            else:
                                logger.info("{} - Query: {}.\n results: {}".format(requester, query, counts))
                        else:
                            logger.info("{} - cypher query: {}".format(requester, query))
                    else:
                        logger.error("Error loading: File does not exist. Query: {}".format(query))
            else:
                result = connector.commitQuery(driver, query+";")
        except Exception as err:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error("Loading: {}, file: {}, line: {} - query: {}".format(err, fname, exc_tb.tb_lineno, query))

    return result


def updateDB(driver, imports=None, specific=[]):
    """
    Populates the graph database with information for each Database, Ontology or Experiment \
    specified in imports. If imports is not defined, the function populates the entire graph \
    database based on the graph variable defined in the grapher_config.py module. \
    This function also updates the graph stats object with numbers from the loaded entities and \
    relationships.

    :param driver: neo4j driver, which provides the connection to the neo4j graph database.
    :type driver: neo4j driver
    :param list imports: a list of entities to be loaded into the graph.
    """
    if imports is None:
        imports = config["graph"]
    try:
        cypher_queries = ckg_utils.get_queries(os.path.join(cwd, config['cypher_queries_file']))
    except Exception as err:
        logger.error("Reading queries file > {}.".format(err))

    for i in imports:
        queries = []
        logger.info("Loading {} into the database".format(i))
        try:
            import_dir = os.path.join(cwd, directories["databasesDirectory"]).replace('\\', '/')
            if i == "ontologies":
                entities = [e.lower() for e in config["ontology_entities"]]
                if len(specific) > 0:
                    entities = list(set(entities).intersection([s.lower() for s in specific]))
                import_dir = os.path.join(cwd, directories["ontologiesDirectory"]).replace('\\', '/')
                ontologyDataImportCode = cypher_queries['IMPORT_ONTOLOGY_DATA']['query']
                for entity in entities:
                    queries.extend(ontologyDataImportCode.replace("ENTITY", entity.capitalize()).replace("IMPORTDIR", import_dir).split(';')[0:-1])
                mappings = config['ontology_mappings']
                mappingImportCode = cypher_queries['IMPORT_ONTOLOGY_MAPPING_DATA']['query']
                for m in mappings:
                    if m.lower() in entities:
                        for r in mappings[m]:
                            queries.extend(mappingImportCode.replace("ENTITY1", m).replace("ENTITY2", r).replace("IMPORTDIR", import_dir).split(';')[0:-1])
                print('Done Loading ontologies')
            elif i == "biomarkers":
                code = cypher_queries['IMPORT_BIOMARKERS']['query']
                import_dir = os.path.join(cwd, directories["curatedDirectory"]).replace('\\', '/')
                queries = code.replace("IMPORTDIR", import_dir).split(';')[0:-1]
                print('Done Loading biomarkers')
            elif i == "qcmarkers":
                code = cypher_queries['IMPORT_QCMARKERS']['query']
                import_dir = os.path.join(cwd, directories["curatedDirectory"]).replace('\\', '/')
                queries = code.replace("IMPORTDIR", import_dir).split(';')[0:-1]
                print('Done Loading qcmarkers')
            elif i == "chromosomes":
                code = cypher_queries['IMPORT_CHROMOSOME_DATA']['query']
                queries = code.replace("IMPORTDIR", import_dir).split(';')[0:-1]
                print('Done Loading chromosomes')
            elif i == "genes":
                code = cypher_queries['IMPORT_GENE_DATA']['query']
                queries = code.replace("IMPORTDIR", import_dir).split(';')[0:-1]
                print('Done Loading genes')
            elif i == "transcripts":
                code = cypher_queries['IMPORT_TRANSCRIPT_DATA']['query']
                queries = code.replace("IMPORTDIR", import_dir).split(';')[0:-1]
                print('Done Loading transcritps')
            elif i == "proteins":
                code = cypher_queries['IMPORT_PROTEIN_DATA']['query']
                queries = code.replace("IMPORTDIR", import_dir).split(';')[0:-1]
                print('Done Loading proteins')
            elif i == "functional_regions":
                code = cypher_queries["IMPORT_FUNCTIONAL_REGIONS"]['query']
                queries = code.replace("IMPORTDIR", import_dir).split(';')[0:-1]
                print('Done Loading functional_regions')
            elif i == "annotations":
                code = cypher_queries['IMPORT_PROTEIN_ANNOTATIONS']['query']
                queries = code.replace("IMPORTDIR", import_dir).split(';')[0:-1]
                print('Done Loading annotations')
            elif i == "complexes":
                code = cypher_queries['IMPORT_COMPLEXES']['query']
                for resource in config["complexes_resources"]:
                    queries.extend(code.replace("IMPORTDIR", import_dir).replace("RESOURCE", resource.lower()).split(';')[0:-1])
                print('Done Loading complexes')
            elif i == "modified_proteins":
                code = cypher_queries['IMPORT_MODIFIED_PROTEINS']['query']
                for resource in config["modified_proteins_resources"]:
                    queries.extend(code.replace("IMPORTDIR", import_dir).replace("RESOURCE", resource.lower()).split(';')[0:-1])
                code = cypher_queries['IMPORT_MODIFIED_PROTEIN_ANNOTATIONS']['query']
                for resource in config["modified_proteins_annotation_resources"]:
                    queries.extend(code.replace("IMPORTDIR", import_dir).replace("RESOURCE", resource.lower()).split(';')[0:-1])
                print('Done Loading modified_proteins')
            elif i == "pathology_expression":
                code = cypher_queries['IMPORT_PATHOLOGY_EXPRESSION']['query']
                for resource in config["pathology_expression_resources"]:
                    queries.extend(code.replace("IMPORTDIR", import_dir).replace("RESOURCE", resource.lower()).split(';')[0:-1])
                print('Done Loading pathology_expression')
            elif i == "ppi":
                code = cypher_queries['IMPORT_CURATED_PPI_DATA']['query']
                for resource in config["curated_PPI_resources"]:
                    queries.extend(code.replace("IMPORTDIR", import_dir).replace("RESOURCE", resource.lower()).split(';')[0:-1])
                code = cypher_queries['IMPORT_COMPILED_PPI_DATA']['query']
                for resource in config["compiled_PPI_resources"]:
                    queries.extend(code.replace("IMPORTDIR", import_dir).replace("RESOURCE", resource.lower()).split(';')[0:-1])
                code = cypher_queries['IMPORT_PPI_ACTION']['query']
                for resource in config["PPI_action_resources"]:
                    queries.extend(code.replace("IMPORTDIR", import_dir).replace("RESOURCE", resource.lower()).split(';')[0:-1])
                print('Done Loading ppi')
            elif i == "protein_structure":
                code = cypher_queries['IMPORT_PROTEIN_STRUCTURES']['query']
                queries = code.replace("IMPORTDIR", import_dir).split(';')[0:-1]
                print('Done Loading protein_structure')
            elif i == "diseases":
                code = cypher_queries['IMPORT_DISEASE_DATA']['query']
                for entity, resource in config["disease_resources"]:
                    queries.extend(code.replace("IMPORTDIR", import_dir).replace("ENTITY", entity).replace("RESOURCE", resource.lower()).split(';')[0:-1])
                print('Done Loading diseases')
            elif i == "drugs":
                code = cypher_queries['IMPORT_DRUG_DATA']['query']
                queries = code.replace("IMPORTDIR", import_dir).split(';')[0:-1]
                code = cypher_queries['IMPORT_DRUG_INTERACTION_DATA']['query']
                for resource in config['drug_drug_interaction_resources']:
                    queries.extend(code.replace("IMPORTDIR", import_dir).replace("RESOURCE", resource.lower()).split(';')[0:-1])
                code = cypher_queries['IMPORT_CURATED_DRUG_DATA']['query']
                for resource in config["curated_drug_resources"]:
                    queries.extend(code.replace("IMPORTDIR", import_dir).replace("RESOURCE", resource.lower()).split(';')[0:-1])
                code = cypher_queries['IMPORT_COMPILED_DRUG_DATA']['query']
                for resource in config["compiled_drug_resources"]:
                    queries.extend(code.replace("IMPORTDIR", import_dir).replace("RESOURCE", resource.lower()).split(';')[0:-1])
                code = cypher_queries['IMPORT_DRUG_ACTS_ON']['query']
                for resource in config["drug_action_resources"]:
                    queries.extend(code.replace("IMPORTDIR", import_dir).replace("RESOURCE", resource.lower()).split(';')[0:-1])
                print('Done Loading drugs')
            elif i == "side_effects":
                code = cypher_queries['IMPORT_DRUG_SIDE_EFFECTS']['query']
                for resource in config["side_effects_resources"]:
                    queries.extend(code.replace("IMPORTDIR", import_dir).replace("RESOURCE", resource.lower()).split(';')[0:-1])
                print('Done Loading side_effects')
            elif i == 'pathway':
                code = cypher_queries['IMPORT_PATHWAY_DATA']['query']
                for resource in config["pathway_resources"]:
                    queries.extend(code.replace("IMPORTDIR", import_dir).replace("RESOURCE", resource.lower()).split(';')[0:-1])
                print('Done Loading pathway')
            elif i == 'metabolite':
                code = cypher_queries['IMPORT_METABOLITE_DATA']['query']
                for resource in config["metabolite_resources"]:
                    queries.extend(code.replace("IMPORTDIR", import_dir).replace("RESOURCE", resource.lower()).split(';')[0:-1])
                print('Done Loading metabolite')
            elif i == 'food':
                code = cypher_queries['IMPORT_FOOD_DATA']['query']
                for resource in config["food_resources"]:
                    queries.extend(code.replace("IMPORTDIR", import_dir).replace("RESOURCE", resource.lower()).split(';')[0:-1])
                print('Done Loading food')
            elif i == "gwas":
                code = cypher_queries['IMPORT_GWAS']['query']
                queries = code.replace("IMPORTDIR", import_dir).split(';')[0:-1]
                code = cypher_queries['IMPORT_VARIANT_FOUND_IN_GWAS']['query']
                queries.extend(code.replace("IMPORTDIR", import_dir).split(';')[0:-1])
                code = cypher_queries['IMPORT_GWAS_STUDIES_TRAIT']['query']
                queries.extend(code.replace("IMPORTDIR", import_dir).split(';')[0:-1])
                print('Done Loading gwas')
            elif i == "known_variants":
                code = cypher_queries['IMPORT_KNOWN_VARIANT_DATA']['query']
                queries = code.replace("IMPORTDIR", import_dir).split(';')[0:-1]
                print('Done Loading known_variants')
            elif i == "clinical_variants":
                code = cypher_queries['IMPORT_CLINICALLY_RELEVANT_VARIANT_DATA']['query']
                for resource in config["clinical_variant_resources"]:
                    queries.extend(code.replace("IMPORTDIR", import_dir).replace("RESOURCE", resource.lower()).split(';')[0:-1])
                print('Done Loading clinical_variants')
            elif i == "jensenlab":
                code = cypher_queries['IMPORT_JENSENLAB_DATA']['query']
                for (entity1, entity2) in config["jensenlabEntities"]:
                    queries.extend(code.replace("IMPORTDIR", import_dir).replace("ENTITY1", entity1).replace("ENTITY2", entity2).split(';')[0:-1])
                print('Done Loading jensenlab')
            elif i == "mentions":
                code = cypher_queries['CREATE_PUBLICATIONS']['query']
                queries = code.replace("IMPORTDIR", import_dir).split(';')[0:-1]
                code = cypher_queries['IMPORT_MENTIONS']['query']
                for entity in config["mentionEntities"]:
                    queries.extend(code.replace("IMPORTDIR", import_dir).replace("ENTITY", entity).split(';')[0:-1])
                print('Done Loading mentions')
            elif i == "published":
                code = cypher_queries['IMPORT_PUBLISHED_IN']['query']
                for entity in config["publicationEntities"]:
                    queries.extend(code.replace("IMPORTDIR", import_dir).replace("ENTITY", entity).split(';')[0:-1])
                print('Done Loading published')
            elif i == "user":
                usersDir = os.path.join(cwd, directories["usersImportDirectory"]).replace('\\', '/')
                user_cypher = cypher_queries['CREATE_USER_NODE']
                code = user_cypher['query']
                queries.extend(code.replace("IMPORTDIR", usersDir).split(';')[0:-1])
                print('Done Loading user')
            elif i == "project":
                import_dir = os.path.join(cwd, directories["experimentsDirectory"]).replace('\\', '/')
                projects = builder_utils.listDirectoryFolders(import_dir)
                if len(specific) > 0:
                    projects = list(set(projects).intersection(specific))
                project_cypher = cypher_queries['IMPORT_PROJECT']
                for project in projects:
                    projectDir = os.path.join(import_dir, project)
                    projectDir = os.path.join(projectDir, 'project').replace('\\','/')
                    for project_section in project_cypher:
                        code = project_section['query']
                        queries.extend(code.replace("IMPORTDIR", projectDir).replace('PROJECTID', project).split(';')[0:-1])
                print('Done Loading project')
            elif i == "experiment":
                import_dir = os.path.join(cwd, directories["experimentsDirectory"]).replace('\\', '/')
                datasets_cypher = cypher_queries['IMPORT_DATASETS']
                projects = builder_utils.listDirectoryFolders(import_dir)
                if len(specific) > 0:
                    projects = list(set(projects).intersection(specific))
                for project in projects:
                    projectDir = os.path.join(import_dir, project).replace('\\', '/')
                    datasetTypes = builder_utils.listDirectoryFolders(projectDir)
                    for dtype in datasetTypes:
                        datasetDir = os.path.join(projectDir, dtype).replace('\\', '/')
                        if dtype in datasets_cypher:
                            dataset = datasets_cypher[dtype]
                            code = dataset['query']
                            queries.extend(code.replace("IMPORTDIR", datasetDir).replace('PROJECTID', project).split(';')[0:-1])
                print('Done Loading experiment')
            else:
                logger.error("Non-existing dataset. The dataset you are trying to load does not exist: {}.".format(i))
            load_into_database(driver, queries, i)
        except Exception as err:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error("Loading: {}: {}, file: {}, line: {}".format(i, err, fname, exc_tb.tb_lineno))


def fullUpdate():
    """
    Main method that controls the population of the graph database. Firstly, it gets a connection \
    to the database (driver) and then initiates the update of the entire database getting \
    all the graph entities to update from configuration. Once the graph database has been \
    populated, the imports folder in data/ is compressed and archived in the archive/ folder \
    so that a backup of the imports files is kept (full).
    """
    imports = config["graph"]
    driver = connector.getGraphDatabaseConnectionConfiguration()
    logger.info("Full update of the database - Updating: {}".format(",".join(imports)))
    updateDB(driver, imports)
    logger.info("Full update of the database - Update took: {}".format(datetime.now() - START_TIME))
    logger.info("Full update of the database - Archiving imports folder")
    archiveImportDirectory(archive_type="full")
    logger.info("Full update of the database - Archiving took: {}".format(datetime.now() - START_TIME))


def partialUpdate(imports, specific=[]):
    """
    Method that controls the update of the graph database with the specified entities and \
    relationships. Firstly, it gets a connection to the database (driver) and then initiates \
    the update of the specified graph entities. \
    Once the graph database has been populated, the data files uploaded to the graph are compressed \
    and archived in the archive/ folder (partial).

    :param list imports: list of entities to update
    """
    driver = connector.getGraphDatabaseConnectionConfiguration()
    logger.info("Partial update of the database - Updating: {}".format(",".join(imports)))
    updateDB(driver, imports, specific)
    logger.info("Partial update of the database - Update took: {}".format(datetime.now() - START_TIME))
    logger.info("Partial update of the database - Archiving imports folder")
    #archiveImportDirectory(archive_type="partial")
    logger.info("Partial update of the database - Archiving {} took: {}".format(",".join(imports), datetime.now() - START_TIME))


def archiveImportDirectory(archive_type="full"):
    """
    This function creates the compressed backup imports folder with either the whole folder \
    (full update) or with only the files uploaded (partial update). The folder or files are \
    compressed into a gzipped tarball file and stored in the archive/ folder defined in the \
    configuration.

    :param str archive_type: whether it is a full update or a partial update.
    """
    dest_folder = directories["archiveDirectory"]
    builder_utils.checkDirectory(dest_folder)
    folder_to_backup = directories["importDirectory"]
    date, time = builder_utils.getCurrentTime()
    file_name = "{}_{}_{}".format(archive_type, date.replace('-', ''), time.replace(':', ''))
    logger.info("Archiving {} to file: {}".format(folder_to_backup, file_name))
    builder_utils.compress_directory(folder_to_backup, dest_folder, file_name)
    logger.info("New backup created: {}".format(file_name))

if __name__ == "__main__":
    fullUpdate()
    #partialUpdate(imports=["clinical variants"])
