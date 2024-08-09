from neo4j import AsyncGraphDatabase
import logging

async def get_driver() -> AsyncGraphDatabase:
    """Get a driver for the Neo4j database."""
    return AsyncGraphDatabase.driver(
        "bolt://localhost:7687", 
        auth=(
            "neo4j", "ari101013!"
        )
    )

async def import_ontologies():
    """Import the ontologies into the Neo4j database."""
    driver = await get_driver()
    async with driver.session() as session:
        try:
            async with session.begin_transaction() as tx:
                for entity in [
                    "Disease", "Tissue", "Clinical_variable", "Phenotype",
                    "Modification", "Molecular_interaction", "Biological_process",
                    "Molecular_function", "Cellular_component", "Experimental_factor",
                ]:
                    try:
                        constraint_query = f"""
                        CREATE CONSTRAINT IF NOT EXISTS FOR (n:{entity}) REQUIRE n.id IS UNIQUE
                        """
                        await tx.run(constraint_query)
                        logging.info(f"Created constraint for {entity}")
                    except Exception as e:
                        logging.error(f"Error creating constraint for {entity}: {e}")

                    for index_type in ['name', 'id', 'type']:
                        try:
                            index_query = f"""
                            CREATE INDEX IF NOT EXISTS FOR (n:{entity}) ON (n.{index_type})
                            """
                            await tx.run(index_query)
                            logging.info(f"Created index for {entity} {index_type}")
                        except Exception as e:
                            logging.error(f"Error creating index for {entity} {index_type}: {e}")

                    try:
                        load_entity_query = f'''
                        CALL {{
                            LOAD CSV WITH HEADERS FROM "file:///{entity}.tsv" as line
                            FIELDTERMINATOR "\\t"
                            MERGE (n:{entity}:ENTITY {{id: line.id}})
                            ON CREATE SET n.name = line.name,
                                            n.description = line.description,
                                            n.type = line.type,
                                            n.synonyms = CASE WHEN line.synonyms IS NOT NULL AND line.synonyms <> ''
                                                THEN split(line.synonyms, ",")
                                                ELSE [] END
                            RETURN COUNT(n) as entity_count
                        }}
                        RETURN entity_count
                        '''
                        result = await tx.run(load_entity_query)
                        records = await result.fetch()
                        if records:
                            logging.info(f"Loaded {records[0]['entity_count']} {entity} entities")
                    except Exception as e:
                        logging.error(f"Error loading {entity} entities: {e}")

                    try:
                        load_has_parent_query = f'''
                        CALL {{
                            LOAD CSV WITH HEADERS FROM "file:///{entity}_has_parent.tsv" as line
                            FIELDTERMINATOR "\\t"
                            MATCH (n1:{entity} {{id: line.START_ID}})
                            MATCH (n2:{entity} {{id: line.END_ID}})
                            MERGE (n1)-[:HAS_PARENT]->(n2)
                            RETURN COUNT(*) as relationship_count
                        }}
                        RETURN relationship_count
                        '''
                        result = await tx.run(load_has_parent_query)
                        records = await result.fetch()
                        if records:
                            logging.info(f"Loaded {records[0]['relationship_count']} HAS_PARENT relationships for {entity}")
                    except Exception as e:
                        logging.error(f"Error loading HAS_PARENT relationships for {entity}: {e}")

                await tx.commit()
                logging.info("Transaction committed successfully")
        except Exception as e:
            logging.error(f"Error in transaction: {e}")
            # No need to explicitly rollback; it's handled automatically
        finally:
            await driver.close()


async def import_biomarkers():
    """Import the biomarkers into the Neo4j database."""
    driver = await get_driver()
    async with driver.session() as session:
        async with session.begin_transaction() as tx:
            try:
                index_query = """
                CREATE INDEX IF NOT EXISTS FOR (n:Disease) ON (n.id);
                CREATE INDEX IF NOT EXISTS FOR (n:Protein) ON (n.id);
                """
                await tx.run(index_query)
                logging.info("Created indexes for Disease and Protein id")
            except Exception as e:
                logging.error(f"Error creating indexes: {e}")

            try:
                biomarker_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///protein_is_biomarker_of_disease.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (p:Protein {id: line.START_ID})
                    MATCH (d:Disease {id: line.END_ID})
                    MERGE (p)-[r:IS_BIOMARKER_OF_DISEASE {
                        is_used_in_clinic: toBoolean(line.used_in_clinic),
                        assay: line.assay,
                        is_routine: toBoolean(line.is_routine),
                        reference: line.reference,
                        source: line.source,
                        age_range: line.age_range,
                        age_units: line.age_units,
                        sex: line.sex,
                        normal_range: line.normal_range,
                        units: line.units,
                        notes: line.notes
                    }]->(d)
                    RETURN COUNT(r) AS biomarker_count
                }
                RETURN biomarker_count;
                '''
                result = await tx.run(biomarker_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['biomarker_count']} biomarkers")
            except Exception as e:
                logging.error(f"Error loading biomarkers: {e}")

    await driver.close()

async def import_qc_markers():
    """Import the QC markers into the Neo4j database."""
    driver = await get_driver()
    async with driver.session() as session:
        async with session.begin_transaction() as tx:
            try:
                index_query = """
                CREATE INDEX IF NOT EXISTS FOR (t:Tissue) ON (t.id);
                CREATE INDEX IF NOT EXISTS FOR (p:Protein) ON (p.id);
                """
                await tx.run(index_query)
                logging.info("Created index for Tissue id and Protein id")
            except Exception as e:
                logging.error(f"Error creating index: {e}")

            try:
                qc_markers_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///protein_is_qcmarker_in_tissue.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (p:Protein {id: line.START_ID})
                    MATCH (t:Tissue {id: line.END_ID})
                    MERGE (p)-[r:IS_QCMARKER_IN_TISSUE {class: line.class}]->(t)
                    RETURN COUNT(r) AS qcmarker_count
                }
                RETURN qcmarker_count
                '''
                result = await tx.run(qc_markers_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['qcmarker_count']} QC markers")
            except Exception as e:
                logging.error(f"Error loading QC markers: {e}")

    await driver.close()

async def import_chromosomes():

    driver = await get_driver()
    async with driver.session() as session:
        async with session.begin_transaction() as tx:
            try:
                constraint_query = '''
                CREATE CONSTRAINT IF NOT EXISTS FOR (c:Chromosome) REQUIRE c.id IS UNIQUE
                '''
                await tx.run(constraint_query)
                logging.info("Created constraint for Chromosome")
            except Exception as e:
                logging.error(f"Error creating constraint: {e}")

            try:
                index_query = '''
                CREATE INDEX IF NOT EXISTS FOR (c:Chromosome) ON (c.id)
                '''
                await tx.run(index_query)
                logging.info("Created index for Chromosome id")
            except Exception as e:
                logging.error(f"Error creating index: {e}")

            try:
                chromosome_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Chromosome.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MERGE (c:Chromosome {id: line.ID})
                    ON CREATE SET c.name = line.name,
                                c.taxid = line.taxid
                    RETURN COUNT(c) AS chromosome_count
                }
                RETURN chromosome_count;
                '''
                result = await tx.run(chromosome_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['chromosome_count']} chromosomes")
            except Exception as e:
                logging.error(f"Error loading chromosomes: {e}")

    await driver.close()

async def import_genes():

    driver = await get_driver()
    async with driver.session() as session:
        async with session.begin_transaction() as tx:
            try:
                constraint_query = '''
                CREATE CONSTRAINT IF NOT EXISTS FOR (g:Gene) REQUIRE g.id IS UNIQUE;
                CREATE CONSTRAINT IF NOT EXISTS FOR (g:Gene) REQUIRE g.name IS UNIQUE;
                '''
                await tx.run(constraint_query)
                logging.info("Created constraint for Gene")
            except Exception as e:
                logging.error(f"Error creating constraint: {e}")

            try:
                index_query = '''
                CREATE INDEX IF NOT EXISTS FOR (g:Gene) ON (g.id);
                CREATE INDEX IF NOT EXISTS FOR (g:Gene) ON (g.name);
                '''
                await tx.run(index_query)
                logging.info("Created index for Gene id")
            except Exception as e:
                logging.error(f"Error creating index: {e}")

            try:
                gene_query = '''
                CALL {
                LOAD CSV WITH HEADERS FROM "file:///Gene.tsv" AS line
                FIELDTERMINATOR '\\t'
                MERGE (g:Gene {id: line.ID})
                ON CREATE SET g.name = line.name,
                            g.family = line.family,
                            g.taxid = line.taxid,
                            g.synonyms = CASE WHEN line.synonyms IS NOT NULL AND line.synonyms <> '' 
                                                THEN split(line.synonyms, ',') 
                                                ELSE [] END
                RETURN COUNT(g) AS gene_count
                }
                RETURN gene_count;
                '''
                result = await tx.run(gene_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['gene_count']} genes")
            except Exception as e:
                logging.error(f"Error loading genes: {e}")

    await driver.close()

async def import_transcripts():

    driver = await get_driver()
    async with driver.session() as session:
        async with session.begin_transaction() as tx:
            try:
                constraint_query = '''
                CREATE CONSTRAINT IF NOT EXISTS FOR (t:Transcript) REQUIRE t.id IS UNIQUE
                '''
                await tx.run(constraint_query)
                logging.info("Created constraint for Transcript")
            except Exception as e:
                logging.error(f"Error creating constraint: {e}")

            try:
                index_query = '''
                CREATE INDEX IF NOT EXISTS FOR (c:Chromosome) ON (c.id);
                CREATE INDEX IF NOT EXISTS FOR (g:Gene) ON (g.id);
                '''
                await tx.run(index_query)
                logging.info("Created index for Chromosome id and Gene id")
            except Exception as e:
                logging.error(f"Error creating index: {e}")

            try:
                transcript_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Transcript.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MERGE (t:Transcript {id: line.ID})
                    ON CREATE SET t.name = line.name,
                                t.class = line.class,
                                t.taxid = line.taxid,
                                t.assembly = line.assembly
                    RETURN COUNT(t) AS transcript_count
                }
                RETURN transcript_count;
                '''
                result = await tx.run(transcript_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['transcript_count']} transcripts")

            except Exception as e:
                logging.error(f"Error loading transcripts: {e}")

            try:
                refseq_located_in_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///refseq_located_in.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (t:Transcript {id: line.START_ID})
                    MATCH (c:Chromosome {id: line.END_ID})
                    MERGE (t)-[r:LOCATED_IN {
                        start: line.start,
                        end: line.end,
                        strand: line.strand
                    }]->(c)
                    RETURN COUNT(r) AS located_in_count
                }
                RETURN located_in_count;
                '''
                result = await tx.run(refseq_located_in_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['located_in_count']} LOCATED_IN relationships")
            except Exception as e:
                logging.error(f"Error loading LOCATED_IN relationships: {e}")

            try:
                refseq_transcribed_into_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///refseq_transcribed_into.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (g:Gene {id: line.START_ID})
                    MATCH (t:Transcript {id: line.END_ID})
                    MERGE (g)-[r:TRANSCRIBED_INTO]->(t)
                    RETURN COUNT(r) AS transcribed_into_count
                }
                RETURN transcribed_into_count;
                '''
                result = await tx.run(refseq_transcribed_into_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['transcribed_into_count']} TRANSCRIBED_INTO relationships")
            except Exception as e:
                logging.error(f"Error loading TRANSCRIBED_INTO relationships: {e}")

    await driver.close()

async def import_proteins():

    driver = await get_driver()
    async with driver.session() as session:
        async with session.begin_transaction() as tx:
            try:
                constraint_query = '''
                CREATE CONSTRAINT IF NOT EXISTS FOR (p:Protein) REQUIRE p.id IS UNIQUE;
                CREATE CONSTRAINT IF NOT EXISTS FOR (a:Amino_acid_sequence) REQUIRE a.id IS UNIQUE;
                CREATE CONSTRAINT IF NOT EXISTS FOR (p:Peptide) REQUIRE p.id IS UNIQUE;
                '''
                await tx.run(constraint_query)
                logging.info("Created constraint for Protein, Amino_acid_sequence, and Peptide")
            except Exception as e:
                logging.error(f"Error creating constraint: {e}")

            try:
                index_query = '''
                CREATE INDEX IF NOT EXISTS FOR (p:Protein) ON (p.name);
                CREATE INDEX IF NOT EXISTS FOR (p:Protein) ON (p.accession);
                CREATE INDEX IF NOT EXISTS FOR (g:Gene) ON (g.id);
                CREATE INDEX IF NOT EXISTS FOR (t:Transcript) ON (t.id);
                CREATE INDEX IF NOT EXISTS FOR (p:Protein) ON (p.id);
                CREATE INDEX IF NOT EXISTS FOR (a:Amino_acid_sequence) ON (a.id);
                CREATE INDEX IF NOT EXISTS FOR (p:Peptide) ON (p.id);
                '''
                await tx.run(index_query)
                logging.info("Created index for Protein name, accession, Gene id, Transcript id, Protein id, Amino_acid_sequence id, and Peptide id")
            except Exception as e:
                logging.error(f"Error creating index: {e}")

            try:
                protein_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Protein.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MERGE (p:Protein {id: line.ID})
                    ON CREATE SET p.accession = line.accession,
                                p.name = line.name,
                                p.description = line.description,
                                p.taxid = line.taxid,
                                p.synonyms = CASE WHEN line.synonyms IS NOT NULL AND line.synonyms <> '' 
                                                    THEN split(line.synonyms, ',') 
                                                    ELSE [] END
                    RETURN COUNT(p) AS protein_count
                }
                RETURN protein_count;
                '''
                result = await tx.run(protein_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['protein_count']} proteins")
            except Exception as e:
                logging.error(f"Error loading proteins: {e}")

            try:
                amino_acid_sequence_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Amino_acid_sequence.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MERGE (aa:Amino_acid_sequence {id: line.ID})
                    ON CREATE SET aa.header = line.header,
                                aa.sequence = line.sequence,
                                aa.size = line.size,
                                aa.source = line.source
                    RETURN COUNT(aa) AS aa_sequence_count
                }
                RETURN aa_sequence_count;
                '''
                result = await tx.run(amino_acid_sequence_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['aa_sequence_count']} amino acid sequences")
            except Exception as e:
                logging.error(f"Error loading amino acid sequences: {e}")

            try:
                protein_has_sequence_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Protein_HAS_Sequence_Amino_acid_sequence.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (p:Protein {id: line.START_ID})
                    MATCH (aa:Amino_acid_sequence {id: line.END_ID})
                    MERGE (p)-[r:HAS_SEQUENCE {source: line.source}]->(aa)
                    RETURN COUNT(r) AS has_sequence_count
                }
                RETURN has_sequence_count;
                '''
                result = await tx.run(protein_has_sequence_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['has_sequence_count']} HAS_SEQUENCE relationships")
            except Exception as e:
                logging.error(f"Error loading HAS_SEQUENCE relationships: {e}")

            try:
                peptide_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Peptide.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MERGE (p:Peptide {id: line.ID})
                    ON CREATE SET p.type = line.type,
                                p.unique = line.unique
                    RETURN COUNT(p) AS peptide_count
                }
                RETURN peptide_count;
                '''
                result = await tx.run(peptide_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['peptide_count']} peptides")
            except Exception as e:
                logging.error(f"Error loading peptides: {e}")

            try:
                peptide_belongs_to_protein_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Peptide_belongs_to_protein.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (p1:Peptide {id: line.START_ID})
                    MATCH (p2:Protein {id: line.END_ID})
                    MERGE (p1)-[r:BELONGS_TO_PROTEIN {source: line.source}]->(p2)
                    RETURN COUNT(r) AS belongs_to_protein_count
                }
                RETURN belongs_to_protein_count;
                '''
                result = await tx.run(peptide_belongs_to_protein_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['belongs_to_protein_count']} BELONGS_TO_PROTEIN relationships")
            except Exception as e:
                logging.error(f"Error loading BELONGS_TO_PROTEIN relationships: {e}")

            try:
                protein_gene_translated_into_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Protein_gene_translated_into.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (g:Gene {id: line.START_ID})
                    MATCH (p:Protein {id: line.END_ID})
                    MERGE (g)-[r:TRANSLATED_INTO]->(p)
                    RETURN COUNT(r) AS gene_translated_into_count
                }
                RETURN gene_translated_into_count;
                '''
                result = await tx.run(protein_gene_translated_into_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['gene_translated_into_count']} TRANSLATED_INTO relationships")
            except Exception as e:
                logging.error(f"Error loading TRANSLATED_INTO relationships: {e}")

            try:
                protein_transcript_translated_into_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Protein_transcript_translated_into.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (t:Transcript {id: line.START_ID})
                    MATCH (p:Protein {id: line.END_ID})
                    MERGE (t)-[r:TRANSLATED_INTO]->(p)
                    RETURN COUNT(r) AS transcript_translated_into_count
                }
                RETURN transcript_translated_into_count;
                '''        
                result = await tx.run(protein_transcript_translated_into_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['transcript_translated_into_count']} TRANSLATED_INTO relationships")
            except Exception as e:
                logging.error(f"Error loading TRANSLATED_INTO relationships: {e}")

    await driver.close()

async def import_transcripts():

    driver = await get_driver()
    async with driver.session() as session:
        async with session.begin_transaction() as tx:
            try:
                constraint_query = '''
                CREATE CONSTRAINT IF NOT EXISTS FOR (t:Transcript) REQUIRE t.id IS UNIQUE
                '''
                await tx.run(constraint_query)
                logging.info("Created constraint for Transcript")
            except Exception as e:
                logging.error(f"Error creating constraint: {e}")

            try:
                index_query = '''
                CREATE INDEX IF NOT EXISTS FOR (c:Chromosome) ON (c.id);
                CREATE INDEX IF NOT EXISTS FOR (t:Transcript) ON (t.id);
                CREATE INDEX IF NOT EXISTS FOR (g:Gene) ON (g.id);
                '''
                await tx.run(index_query)
                logging.info("Created index for Chromosome id and Gene id and Transcript id")
            except Exception as e:
                logging.error(f"Error creating index: {e}")

            try:
                transcript_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Transcript.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MERGE (t:Transcript {id: line.ID})
                    ON CREATE SET t.name = line.name,
                                t.class = line.class,
                                t.taxid = line.taxid,
                                t.assembly = line.assembly
                    RETURN COUNT(t) AS transcript_count
                }
                RETURN transcript_count;
                '''
                result = await tx.run(transcript_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['transcript_count']} transcripts")
            except Exception as e:
                logging.error(f"Error loading transcripts: {e}")

            try:
                refseq_located_in_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///refseq_located_in.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (t:Transcript {id: line.START_ID})
                    MATCH (c:Chromosome {id: line.END_ID})
                    MERGE (t)-[r:LOCATED_IN {
                        start: line.start,
                        end: line.end,
                        strand: line.strand
                    }]->(c)
                    RETURN COUNT(r) AS located_in_count
                }
                RETURN located_in_count;
                '''
                result = await tx.run(refseq_located_in_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['located_in_count']} LOCATED_IN relationships")
            except Exception as e:
                logging.error(f"Error loading LOCATED_IN relationships: {e}")

            try:
                refseq_transcribed_into_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///refseq_transcribed_into.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (g:Gene {id: line.START_ID})
                    MATCH (t:Transcript {id: line.END_ID})
                    MERGE (g)-[r:TRANSCRIBED_INTO]->(t)
                    RETURN COUNT(r) AS transcribed_into_count
                }
                RETURN transcribed_into_count;
                '''
                result = await tx.run(refseq_transcribed_into_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['transcribed_into_count']} TRANSCRIBED_INTO relationships")
            except Exception as e:
                logging.error(f"Error loading TRANSCRIBED_INTO relationships: {e}")

    await driver.close()

async def import_functional_regions():
    
    driver = await get_driver()
    async with driver.session() as session:
        async with session.begin_transaction() as tx:
            try:
                constraint_query = '''
                CREATE CONSTRAINT IF NOT EXISTS FOR (f:Functional_region) REQUIRE f.id IS UNIQUE
                '''
                await tx.run(constraint_query)
                logging.info("Created constraint for Functional_region")
            except Exception as e:
                logging.error(f"Error creating constraint: {e}")

            try:
                index_query = '''
                CREATE INDEX IF NOT EXISTS FOR (f:Functional_region) ON (f.id);
                CREATE INDEX IF NOT EXISTS FOR (p:Publication) ON (p.id);
                '''
                await tx.run(index_query)
                logging.info("Created index for Functional_region id and Publication id")
            except Exception as e:
                logging.error(f"Error creating index: {e}")

            try:
                functional_region_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Functional_region.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MERGE (f:Functional_region {id: line.ID})
                    ON CREATE SET f.name = line.name,
                                f.description = line.description,
                                f.source = line.source
                    RETURN COUNT(f) AS functional_region_count
                }
                RETURN functional_region_count;
                '''
                result = await tx.run(functional_region_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['functional_region_count']} functional regions")
            except Exception as e:
                logging.error(f"Error loading functional regions: {e}")

            try:
                functional_region_found_in_protein_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Functional_region_found_in_protein.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (f:Functional_region {id: line.START_ID})
                    MATCH (p:Protein {id: line.END_ID})
                    MERGE (f)-[r:FOUND_IN_PROTEIN {
                        start: line.start,
                        end: line.end,
                        alignment: line.sequence,
                        source: line.source
                    }]->(p)
                    RETURN COUNT(r) AS found_in_protein_count
                }
                RETURN found_in_protein_count;
                '''
                result = await tx.run(functional_region_found_in_protein_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['found_in_protein_count']} FOUND_IN_PROTEIN relationships")
            except Exception as e:
                logging.error(f"Error loading FOUND_IN_PROTEIN relationships: {e}")

            try:
                functional_region_mentioned_in_publication_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Functional_region_mentioned_in_publication.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (f:Functional_region {id: line.START_ID})
                    MATCH (p:Publication {id: line.END_ID})
                    MERGE (f)-[r:MENTIONED_IN_PUBLICATION {source: line.source}]-(p)
                    RETURN COUNT(r) AS mentioned_in_publication_count
                }
                RETURN mentioned_in_publication_count;
                '''
                result = await tx.run(functional_region_mentioned_in_publication_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['mentioned_in_publication_count']} MENTIONED_IN_PUBLICATION relationships")
            except Exception as e:
                logging.error(f"Error loading MENTIONED_IN_PUBLICATION relationships: {e}")

    await driver.close()

async def import_annotations():

    driver = await get_driver()

    async with driver.session() as session:
        async with session.begin_transaction() as tx:
            try:
                index_query = '''
                CREATE INDEX IF NOT EXISTS FOR (c:Cellular_component) ON (c.id);
                CREATE INDEX IF NOT EXISTS FOR (f:Molecular_function) ON (f.id);
                CREATE INDEX IF NOT EXISTS FOR (b:Biological_process) ON (b.id); 
                CREATE INDEX IF NOT EXISTS FOR (p:Protein) ON (p.id);
                '''
                await tx.run(index_query)
                logging.info("Created indexes for Cellular_component id, Molecular_function id, Biological_process id, and Protein id")
            except Exception as e:
                logging.error(f"Error creating indexes: {e}")

            try:
                cellular_component_associated_with_protein_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Cellular_component_associated_with.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (p:Protein {id: line.START_ID})
                    MATCH (c:Cellular_component {id: line.END_ID})
                    MERGE (p)-[r:ASSOCIATED_WITH {
                        score: toFloat(line.score),
                        source: line.source,
                        evidence_type: line.evidence_type
                    }]->(c)
                    RETURN COUNT(r) AS cellular_component_count
                }
                RETURN cellular_component_count;
                '''
                result = await tx.run(cellular_component_associated_with_protein_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['cellular_component_count']} ASSOCIATED_WITH relationships")
            except Exception as e:
                logging.error(f"Error loading ASSOCIATED_WITH relationships: {e}")

            try:
                molecular_function_associated_with_protein_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Molecular_function_associated_with.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (p:Protein {id: line.START_ID})
                    MATCH (f:Molecular_function {id: line.END_ID})
                    MERGE (p)-[r:ASSOCIATED_WITH {
                        score: toFloat(line.score),
                        source: line.source,
                        evidence_type: line.evidence_type
                    }]->(f)
                    RETURN COUNT(r) AS molecular_function_count
                }
                RETURN molecular_function_count;
                '''
                result = await tx.run(molecular_function_associated_with_protein_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['molecular_function_count']} ASSOCIATED_WITH relationships")
            except Exception as e:
                logging.error(f"Error loading ASSOCIATED_WITH relationships: {e}")

            try:
                biological_process_associated_with_protein_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Biological_process_associated_with.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (p:Protein {id: line.START_ID})
                    MATCH (b:Biological_process {id: line.END_ID})
                    MERGE (p)-[r:ASSOCIATED_WITH {
                        score: toFloat(line.score),
                        source: line.source,
                        evidence_type: line.evidence_type
                    }]->(b)
                    RETURN COUNT(r) AS biological_process_count
                }
                RETURN biological_process_count;
                '''
                result = await tx.run(biological_process_associated_with_protein_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['biological_process_count']} ASSOCIATED_WITH relationships")
            except Exception as e:
                logging.error(f"Error loading ASSOCIATED_WITH relationships: {e}")

    await driver.close()

async def import_complexes():

    driver = await get_driver()
    async with driver.session() as session:
        async with session.begin_transaction() as tx:
            try:
                constraint_query = '''
                CREATE CONSTRAINT IF NOT EXISTS FOR (c:Complex) REQUIRE c.id IS UNIQUE
                '''
                await tx.run(constraint_query)
                logging.info("Created constraint for Complex")
            except Exception as e:
                logging.error(f"Error creating constraint: {e}")

            try:
                index_query = '''
                CREATE INDEX IF NOT EXISTS FOR (c:Complex) ON (c.id);
                CREATE INDEX IF NOT EXISTS FOR (p:Protein) ON (p.id);
                CREATE INDEX IF NOT EXISTS FOR (b:Biological_process) ON (b.id);
                '''
                await tx.run(index_query)
                logging.info("Created index for Complex id, Protein id, and Biological_process id")
            except Exception as e:
                logging.error(f"Error creating index: {e}")

            try:
                complex_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Complex.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MERGE (c:Complex {id: line.ID})
                    ON CREATE SET c.name = line.name,
                                c.organism = line.organism,
                                c.source = line.source,
                                c.synonyms = CASE WHEN line.synonyms IS NOT NULL AND line.synonyms <> '' 
                                                    THEN split(line.synonyms, ',') 
                                                    ELSE [] END
                    RETURN COUNT(c) AS complex_count
                }
                RETURN complex_count;
                '''
                result = await tx.run(complex_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['complex_count']} complexes")
            except Exception as e:
                logging.error(f"Error loading complexes: {e}")

            try:
                protein_is_subunit_of_complex_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///corum_protein_is_subunit_of.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (p:Protein {id: line.START_ID})
                    MATCH (c:Complex {id: line.END_ID})
                    MERGE (p)-[r:IS_SUBUNIT_OF {
                        cell_lines: CASE WHEN line.cell_lines IS NOT NULL AND line.cell_lines <> '' 
                                        THEN split(line.cell_lines, ',') 
                                        ELSE [] END,
                        evidences: CASE WHEN line.evidences IS NOT NULL AND line.evidences <> '' 
                                        THEN split(line.evidences, ',') 
                                        ELSE [] END,
                        publication: line.publication,
                        source: line.source
                    }]->(c)
                    RETURN COUNT(r) AS is_subunit_of_count
                }
                RETURN is_subunit_of_count;
                '''
                result = await tx.run(protein_is_subunit_of_complex_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['is_subunit_of_count']} IS_SUBUNIT_OF relationships")
            except Exception as e:
                logging.error(f"Error loading IS_SUBUNIT_OF relationships: {e}")

            try:
                biological_process_associated_with_complex_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///corum_biological_process_associated_with.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (c:Complex {id: line.START_ID})
                    MATCH (b:Biological_process {id: line.END_ID})
                    MERGE (c)-[r:ASSOCIATED_WITH {
                        evidence_type: line.evidence_type,
                        score: toFloat(line.score),
                        source: line.source
                    }]->(b)
                    RETURN COUNT(r) AS complex_associated_with_count
                }
                RETURN complex_associated_with_count;
                '''
                result = await tx.run(biological_process_associated_with_complex_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['complex_associated_with_count']} ASSOCIATED_WITH relationships")
            except Exception as e:
                logging.error(f"Error loading ASSOCIATED_WITH relationships: {e}")

    await driver.close()

async def import_modified_proteins():
    """SKip for now"""
    pass

async def import_pathology_expression():

    driver = await get_driver()
    async with driver.session() as session:
        async with session.begin_transaction() as tx:
            try:
                index_query = '''
                CREATE INDEX IF NOT EXISTS FOR (d:Disease) ON (d.id);
                CREATE INDEX IF NOT EXISTS FOR (p:Protein) ON (p.id);
                '''
                await tx.run(index_query)
                logging.info("Created indexes for Disease id and Protein id")
            except Exception as e:
                logging.error(f"Error creating indexes: {e}")

            try:
                preotein_detected_in_disease_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///hpa_protein_detected_in_pathology_sample.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (p:Protein {id: line.START_ID})
                    MATCH (d:Disease {id: line.END_ID})
                    MERGE (p)-[r:DETECTED_IN_PATHOLOGY_SAMPLE {
                        expression_high: line.expression_high,
                        expression_medium: line.expression_medium,
                        expression_low: line.expression_low,
                        not_detected: line.not_detected,
                        positive_prognosis_logrank_pvalue: line.positive_prognosis_logrank_pvalue,
                        negative_prognosis_logrank_pvalue: line.negative_prognosis_logrank_pvalue,
                        linkout: line.linkout,
                        source: line.source
                    }]->(d)
                    RETURN COUNT(r) AS detected_in_pathology_sample_count
                }
                RETURN detected_in_pathology_sample_count;
                '''
                result = await tx.run(preotein_detected_in_disease_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['detected_in_pathology_sample_count']} DETECTED_IN_PATHOLOGY_SAMPLE relationships")
            except Exception as e:
                logging.error(f"Error loading DETECTED_IN_PATHOLOGY_SAMPLE relationships: {e}")

    await driver.close()

async def import_ppi():

    driver = await get_driver()
    async with driver.session() as session:
        async with session.begin_transaction() as tx:
            try:
                index_query = '''
                CREATE INDEX IF NOT EXISTS FOR (p:Protein) ON (p.id);
                '''
                await tx.run(index_query)
                logging.info("Created index for Protein id")
            except Exception as e:
                logging.error(f"Error creating index: {e}")

            try:
                curated_ppi_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///intact_interacts_with.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (p1:Protein {id: line.START_ID})
                    MATCH (p2:Protein {id: line.END_ID})
                    MERGE (p1)-[r:CURATED_INTERACTS_WITH {
                        score: toFloat(line.score),
                        interaction_type: line.interaction_type,
                        method: CASE WHEN line.method IS NOT NULL AND line.method <> '' 
                                    THEN split(line.method, ',') 
                                    ELSE [] END,
                        source: CASE WHEN line.source IS NOT NULL AND line.source <> '' 
                                    THEN split(line.source, ',') 
                                    ELSE [] END,
                        evidence: CASE WHEN line.publications IS NOT NULL AND line.publications <> '' 
                                    THEN split(line.publications, ',') 
                                    ELSE [] END
                    }]->(p2)
                    RETURN COUNT(r) AS curated_ppi_count
                }
                RETURN curated_ppi_count;
                '''
                result = await tx.run(curated_ppi_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['curated_ppi_count']} CURATED_INTERACTS_WITH relationships")
            except Exception as e:
                logging.error(f"Error loading CURATED_INTERACTS_WITH relationships: {e}")

            try:
                compiled_ppi_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///string_interacts_with.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (p1:Protein {id: line.START_ID})
                    MATCH (p2:Protein {id: line.END_ID})
                    MERGE (p1)-[r:COMPILED_INTERACTS_WITH {
                        score: toFloat(line.score),
                        interaction_type: line.interaction_type,
                        source: CASE WHEN line.source IS NOT NULL AND line.source <> '' 
                                    THEN split(line.source, ',') 
                                    ELSE [] END,
                        scores: CASE WHEN line.scores IS NOT NULL AND line.scores <> '' 
                                    THEN split(line.scores, ',') 
                                    ELSE [] END,
                        evidence: CASE WHEN line.evidence IS NOT NULL AND line.evidence <> '' 
                                    THEN split(line.evidence, ',') 
                                    ELSE [] END
                    }]->(p2)
                    RETURN COUNT(r) AS compiled_ppi_count
                }
                RETURN compiled_ppi_count;
                '''
                result = await tx.run(compiled_ppi_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['compiled_ppi_count']} COMPILED_INTERACTS_WITH relationships")
            except Exception as e:
                logging.error(f"Error loading COMPILED_INTERACTS_WITH relationships: {e}")

            try:
                acts_on_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///string_protein_acts_on_protein.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (p1:Protein {id: line.START_ID})
                    MATCH (p2:Protein {id: line.END_ID})
                    MERGE (p1)-[r:ACTS_ON {
                        source: line.source,
                        action: line.action,
                        score: toFloat(line.score),
                        directionality: toBoolean(line.directionality)
                    }]->(p2)
                    RETURN COUNT(r) AS ppi_action_count
                }
                RETURN ppi_action_count;
                '''
                result = await tx.run(acts_on_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['ppi_action_count']} ACTS_ON relationships")
            except Exception as e:
                logging.error(f"Error loading ACTS_ON relationships: {e}")

    await driver.close()

async def import_protein_structure():

    driver = await get_driver()
    async with driver.session() as session:
        async with session.begin_transaction() as tx:
            try:
                constraint_query = '''
                CREATE CONSTRAINT IF NOT EXISTS FOR (p:Protein_structure) REQUIRE p.id IS UNIQUE;
                '''
                await tx.run(constraint_query)
                logging.info("Created constraint for Protein_structure")
            except Exception as e:
                logging.error(f"Error creating constraint: {e}")

            try:
                index_query = '''
                CREATE INDEX IF NOT EXISTS FOR (p:Protein_structure) ON (p.id);
                CREATE INDEX IF NOT EXISTS FOR (p:Protein) ON (p.id);
                '''
                await tx.run(index_query)
                logging.info("Created index for Protein_structure id and Protein id")

            except Exception as e:
                logging.error(f"Error creating index: {e}")

            try:
                protein_structure_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Protein_structures.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MERGE (s:Protein_structure {id: line.ID})
                    ON CREATE SET s.source = line.source,
                                s.link = line.link
                    RETURN COUNT(s) AS protein_structure_count
                }
                RETURN protein_structure_count;
                '''
                result = await tx.run(protein_structure_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['protein_structure_count']} protein structures")
            except Exception as e:
                logging.error(f"Error loading protein structures: {e}")

            try:
                protein_has_structure_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Protein_has_structure.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (p:Protein {id: line.START_ID})
                    MATCH (s:Protein_structure {id: line.END_ID})
                    MERGE (p)-[r:HAS_STRUCTURE {source: line.source}]->(s)
                    RETURN COUNT(r) AS has_structure_count
                }
                RETURN has_structure_count;
                '''
                result = await tx.run(protein_has_structure_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['has_structure_count']} HAS_STRUCTURE relationships")
            except Exception as e:
                logging.error(f"Error loading HAS_STRUCTURE relationships: {e}")

    await driver.close()

async def import_diseases():

    driver = await get_driver()
    async with driver.session() as session:
        async with session.begin_transaction() as tx:
            try:
                index_query = '''
                CREATE INDEX IF NOT EXISTS FOR (e:ENTITY) ON (e.id);
                CREATE INDEX IF NOT EXISTS FOR (d:Disease) ON (d.id);
                '''

                await tx.run(index_query)
                logging.info("Created indexes for ENTITY id and Disease id")
            except Exception as e:
                logging.error(f"Error creating indexes: {e}")

            try:
                associated_with_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///disgenet_associated_with.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (e:ENTITY {id: line.START_ID})
                    MATCH (d:Disease {id: line.END_ID})
                    MERGE (e)-[r:ASSOCIATED_WITH {
                        score: toFloat(line.score),
                        evidence_type: line.evidence_type,
                        source: line.source
                    }]->(d)
                    RETURN COUNT(r) AS disease_association_count
                }
                RETURN disease_association_count;
                '''
                result = await tx.run(associated_with_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['disease_association_count']} ASSOCIATED_WITH relationships")
            except Exception as e:
                logging.error(f"Error loading ASSOCIATED_WITH relationships: {e}")

        await driver.close()

async def import_drugs():

    driver = await get_driver()

    async with driver.session() as session:
        async with session.begin_transaction() as tx:
            try:
                constraint_query = '''
                CREATE CONSTRAINT IF NOT EXISTS FOR (d:Drug) REQUIRE d.id IS UNIQUE;
                CREATE CONSTRAINT IF NOT EXISTS FOR (d:Drug) REQUIRE d.name IS UNIQUE;
                '''
                await tx.run(constraint_query)
                logging.info("Created constraint for Drug")
            except Exception as e:
                logging.error(f"Error creating constraint: {e}")

            try:
                index_query = '''
                CREATE INDEX IF NOT EXISTS FOR (d:Drug) ON (d.id);
                CREATE INDEX IF NOT EXISTS FOR (d:Drug) ON (d.name);
                CREATE INDEX IF NOT EXISTS FOR (p:Protein) ON (p.id);
                CREATE INDEX IF NOT EXISTS FOR (g:Gene) ON (g.id);
                '''
                await tx.run(index_query)
                logging.info("Created index for Drug id, Drug name, Protein id, and Gene id")
            except Exception as e:
                logging.error(f"Error creating index: {e}")

            try:
                drug_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Drug.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MERGE (d:Drug {id: line.ID})
                    ON CREATE SET 
                        d.name = line.name,
                        d.description = line.description,
                        d.kingdom = line.kingdom,
                        d.superclass = line.superclass,
                        d.class = line.class,
                        d.subclass = line.subclass,
                        d.indication = line.indication,
                        d.synonyms = line.synonyms,
                        d.mechanism_of_action = line.mechanism_of_action,
                        d.metabolism = line.metabolism,
                        d.pharmacodynamics = line.pharmacodynamics,
                        d.prices = line.prices,
                        d.route_of_elimination = line.route_of_elimination,
                        d.toxicity = line.toxicity,
                        d.absorption = line.absorption,
                        d.half_life = line.half_life,
                        d.groups = line.groups,
                        d.experimental_properties = line.experimental_properties,
                        d.Melting_Point = line.Melting_Point,
                        d.Hydrophobicity = line.Hydrophobicity,
                        d.Isoelectric_Point = line.Isoelectric_Point,
                        d.Molecular_Weight = line.Molecular_Weight,
                        d.Molecular_Formula = line.Molecular_Formula,
                        d.Water_Solubility = line.Water_Solubility,
                        d.Monoisotopic_Weight = line.Monoisotopic_Weight,
                        d.Polar_Surface_Area_PSA = line.Polar_Surface_Area_PSA,
                        d.Refractivity = line.Refractivity,
                        d.Polarizability = line.Polarizability,
                        d.Rotatable_Bond_Count = line.Rotatable_Bond_Count,
                        d.H_Bond_Acceptor_Count = line.H_Bond_Acceptor_Count,
                        d.H_Bond_Donor_Count = line.H_Bond_Donor_Count,
                        d.pKa_strongest_acidic = line.pKa_strongest_acidic,
                        d.pKa_strongest_basic = line.pKa_strongest_basic,
                        d.Physiological_Charge = line.Physiological_Charge,
                        d.Number_of_Rings = line.Number_of_Rings,
                        d.Bioavailability = line.Bioavailability,
                        d.Rule_of_Five = line.Rule_of_Five,
                        d.Ghose_Filter = line.Ghose_Filter,
                        d.MDDR_Like_Rule = line.MDDR_Like_Rule,
                        d.caco2_Permeability = line.caco2_Permeability,
                        d.pKa = line.pKa,
                        d.Boiling_Point = line.Boiling_Point
                    RETURN COUNT(d) AS drug_count
                }
                RETURN drug_count;
                '''
                result = await tx.run(drug_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['drug_count']} drugs")
            except Exception as e:
                logging.error(f"Error loading drugs: {e}")

            try:
                drug_interaction_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///drugbank_interacts_with_drug.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (d1:Drug {id: line.START_ID})
                    MATCH (d2:Drug {id: line.END_ID})
                    MERGE (d1)-[r:INTERACTS_WITH {
                        source: line.source,
                        interaction_type: line.interaction_type
                    }]->(d2)
                    RETURN COUNT(r) AS drug_interaction_count
                }
                RETURN drug_interaction_count;
                '''
                result = await tx.run(drug_interaction_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['drug_interaction_count']} INTERACTS_WITH relationships")
            except Exception as e:
                logging.error(f"Error loading INTERACTS_WITH relationships: {e}")

            try:
                curated_drug_data_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///oncokb_targets.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (d:Drug {id: line.START_ID})
                    MATCH (g:Gene {id: line.END_ID})
                    MERGE (d)-[r:CURATED_TARGETS {
                        source: line.source,
                        interaction_type: line.type,
                        evidence: line.evidence,
                        response: line.response,
                        disease: line.disease,
                        score: line.type
                    }]->(g)
                    RETURN COUNT(r) AS curated_drug_target_count
                }
                RETURN curated_drug_target_count;
                '''
                result = await tx.run(curated_drug_data_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['curated_drug_target_count']} CURATED_TARGETS relationships")
            except Exception as e:
                logging.error(f"Error loading CURATED_TARGETS relationships: {e}")


            try:
                compiled_drug_data_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///stitch_associated_with.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (d:Drug {id: line.START_ID})
                    MATCH (p:Protein {id: line.END_ID})
                    MERGE (d)-[r:COMPILED_TARGETS {
                        score: toFloat(line.score),
                        source: line.source,
                        interaction_type: line.interaction_type,
                        scores: CASE WHEN line.scores IS NOT NULL AND line.scores <> '' 
                                    THEN split(line.scores, ',') 
                                    ELSE [] END,
                        evidences: CASE WHEN line.evidence IS NOT NULL AND line.evidence <> '' 
                                        THEN split(line.evidence, ',') 
                                        ELSE [] END
                    }]->(p)
                    RETURN COUNT(r) AS compiled_drug_target_count
                }
                RETURN compiled_drug_target_count;
                '''
                result = await tx.run(compiled_drug_data_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['compiled_drug_target_count']} COMPILED_TARGETS relationships")
            except Exception as e:
                logging.error(f"Error loading COMPILED_TARGETS relationships: {e}")

            try:
                drug_acts_on_gene_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///stitch_drug_acts_on_protein.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (d:Drug {id: line.START_ID})
                    MATCH (p:Protein {id: line.END_ID})
                    MERGE (d)-[r:ACTS_ON {
                        source: line.source,
                        action: line.action,
                        score: toFloat(line.score),
                        directionality: toBoolean(line.directionality)
                    }]->(p)
                    RETURN COUNT(r) AS drug_acts_on_count
                }
                RETURN drug_acts_on_count;
                '''
                result = await tx.run(drug_acts_on_gene_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['drug_acts_on_count']} ACTS_ON relationships")
            except Exception as e:
                logging.error(f"Error loading ACTS_ON relationships: {e}")

    await driver.close()

async def import_side_effects():

    driver = await get_driver()
    async with driver.session() as session:
        async with session.begin_transaction() as tx:

            try:
                index_query = '''
                CREATE INDEX IF NOT EXISTS FOR (d:Drug) ON (d.id);
                CREATE INDEX IF NOT EXISTS FOR (p:Phenotype) ON (p.id);
                '''
                await tx.run(index_query)
                logging.info("Created indexes for Drug id and phenotype id")

            except Exception as e:
                logging.error(f"Error creating indexes: {e}")

            try:
                side_effect_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///sider_has_side_effect.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (d:Drug {id: line.START_ID})
                    MATCH (p:Phenotype {id: line.END_ID})
                    MERGE (d)-[r:HAS_SIDE_EFFECT]->(p)
                    SET r.source = line.source,
                        r.original_side_effect_code = line.original_side_effect,
                        r.evidence_from = line.evidence_from
                    RETURN COUNT(r) AS side_effect_count
                }
                RETURN side_effect_count;
                '''
                result = await tx.run(side_effect_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['side_effect_count']} HAS_SIDE_EFFECT relationships")
            except Exception as e:
                logging.error(f"Error loading HAS_SIDE_EFFECT relationships: {e}")

            try:
                is_indicated_for_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///sider_is_indicated_for.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (d:Drug {id: line.START_ID})
                    MATCH (p:Phenotype {id: line.END_ID})
                    MERGE (d)-[r:IS_INDICATED_FOR]->(p)
                    SET r.source = line.source,
                        r.original_side_effect_code = line.original_side_effect,
                        r.evidence = line.evidence
                    RETURN COUNT(r) AS indicated_for_count
                }
                RETURN indicated_for_count;
                '''
                result = await tx.run(is_indicated_for_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['indicated_for_count']} IS_INDICATED_FOR relationships")
            except Exception as e:
                logging.error(f"Error loading IS_INDICATED_FOR relationships: {e}")

    await driver.close()

async def import_pathways():

    driver = await get_driver()

    async with driver.session() as session:
        async with session.begin_transaction() as tx:

            try:
                constraint_query = '''
                CREATE CONSTRAINT IF NOT EXISTS FOR (p:Pathway) REQUIRE p.id IS UNIQUE;
                '''

                await tx.run(constraint_query)
                logging.info("Created constraint for Pathway")
            except Exception as e:
                logging.error(f"Error creating constraint: {e}")

            try:
                index_query = '''
                CREATE INDEX IF NOT EXISTS FOR (p:Pathway) ON (p.id);
                CREATE INDEX IF NOT EXISTS FOR (p:Protein) ON (p.id);
                CREATE INDEX IF NOT EXISTS FOR (m:Metabolite) ON (m.id);
                CREATE INDEX IF NOT EXISTS FOR (d:Drug) ON (d.id);
                '''
                await tx.run(index_query)
                logging.info("Created index for Pathway id, Protein id, Metabolite id, and Drug id")
            except Exception as e:
                logging.error(f"Error creating index: {e}")

            try:
                smpdb_pathway_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///smpdb_protein_annotated_to_pathway.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (p:Protein {id: line.START_ID})
                    MATCH (a:Pathway {id: line.END_ID})
                    MERGE (p)-[r:ANNOTATED_IN_PATHWAY {
                        evidence: line.evidence,
                        organism: line.organism,
                        cellular_component: line.cellular_component,
                        source: line.source
                    }]->(a)
                    RETURN COUNT(r) AS protein_pathway_count
                }
                RETURN protein_pathway_count;
                '''
                result = await tx.run(smpdb_pathway_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['protein_pathway_count']} ANNOTATED_IN_PATHWAY relationships")
            except Exception as e:
                logging.error(f"Error loading ANNOTATED_IN_PATHWAY relationships: {e}")

            try:
                reactome_pathway_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///reactome_protein_annotated_to_pathway.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (p:Protein {id: line.START_ID})
                    MATCH (a:Pathway {id: line.END_ID})
                    MERGE (p)-[r:ANNOTATED_IN_PATHWAY {
                        evidence: line.evidence,
                        organism: line.organism,
                        cellular_component: line.cellular_component,
                        source: line.source
                    }]->(a)
                    RETURN COUNT(r) AS protein_pathway_count
                }
                RETURN protein_pathway_count;
                '''
                result = await tx.run(reactome_pathway_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['protein_pathway_count']} ANNOTATED_IN_PATHWAY relationships")
            except Exception as e:
                logging.error(f"Error loading ANNOTATED_IN_PATHWAY relationships: {e}")

            try:
                smpdb_pathway_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///smpdb_Pathway.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MERGE (p:Pathway {id: line.ID})
                    ON CREATE SET p.name = line.name,
                                p.description = line.description,
                                p.organism = line.organism,
                                p.linkout = line.linkout,
                                p.source = line.source
                    RETURN COUNT(p) AS pathway_count
                }
                RETURN pathway_count;
                '''
                result = await tx.run(smpdb_pathway_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['pathway_count']} pathways")
            except Exception as e:
                logging.error(f"Error loading pathways: {e}")

            try:
                smpdb_metabolite_pathway_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///smpdb_metabolite_annotated_to_pathway.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (m:Metabolite {id: line.START_ID})
                    MATCH (a:Pathway {id: line.END_ID})
                    MERGE (m)-[r:ANNOTATED_IN_PATHWAY {
                        evidence: line.evidence,
                        organism: line.organism,
                        cellular_component: line.cellular_component,
                        source: line.source
                    }]->(a)
                    RETURN COUNT(r) AS metabolite_pathway_count
                }
                RETURN metabolite_pathway_count;
                '''
                result = await tx.run(smpdb_metabolite_pathway_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['metabolite_pathway_count']} ANNOTATED_IN_PATHWAY relationships")
            except Exception as e:
                logging.error(f"Error loading ANNOTATED_IN_PATHWAY relationships: {e}")

            try:
                reactome_metabolite_pathway_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///reactome_metabolite_annotated_to_pathway.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (m:Metabolite {id: line.START_ID})
                    MATCH (a:Pathway {id: line.END_ID})
                    MERGE (m)-[r:ANNOTATED_IN_PATHWAY {
                        evidence: line.evidence,
                        organism: line.organism,
                        cellular_component: line.cellular_component,
                        source: line.source
                    }]->(a)
                    RETURN COUNT(r) AS metabolite_pathway_count
                }
                RETURN metabolite_pathway_count;
                '''
                result = await tx.run(reactome_metabolite_pathway_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['metabolite_pathway_count']} ANNOTATED_IN_PATHWAY relationships")
            except Exception as e:
                logging.error(f"Error loading ANNOTATED_IN_PATHWAY relationships: {e}")

            """
            try:
                smpdb_drug_pathway_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///RESOURCE_drug_annotated_to_pathway.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (d:Drug {id: line.START_ID})
                    MATCH (a:Pathway {id: line.END_ID})
                    MERGE (d)-[r:ANNOTATED_IN_PATHWAY {
                        evidence: line.evidence,
                        organism: line.organism,
                        cellular_component: line.cellular_component,
                        source: line.source
                    }]->(a)
                    RETURN COUNT(r) AS drug_pathway_count
                }
                RETURN drug_pathway_count;
                '''
                result = await tx.run(smpdb_drug_pathway_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['drug_pathway_count']} ANNOTATED_IN_PATHWAY relationships")
            except Exception as e:
                logging.error(f"Error loading ANNOTATED_IN_PATHWAY relationships: {e}")
            """

    await driver.close()

async def import_metabolites():

    driver = await get_driver()
    async with driver.session() as session:
        async with session.begin_transaction() as tx:
            try:
                constraint_query = '''
                CREATE CONSTRAINT IF NOT EXISTS FOR (m:Metabolite) REQUIRE m.id IS UNIQUE;
                '''
                await tx.run(constraint_query)
                logging.info("Created constraint for Metabolite")
            except Exception as e:
                logging.error(f"Error creating constraint: {e}")

            try:
                index_query = '''
                CREATE INDEX IF NOT EXISTS FOR (m:Metabolite) ON (m.id);
                CREATE INDEX IF NOT EXISTS FOR (m:Metabolite) ON (m.name);
                CREATE INDEX IF NOT EXISTS FOR (p:Protein) ON (p.id);
                '''
                await tx.run(index_query)
                logging.info("Created index for Metabolite id, Metabolite name, Protein id, and Drug id")
            except Exception as e:
                logging.error(f"Error creating index: {e}")

            try:
                metabolite_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Metabolite.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MERGE (m:Metabolite {id: line.ID})
                    ON CREATE SET m.name = line.name,
                                m.synonyms = line.synonyms,
                                m.description = line.description,
                                m.direct_parent = line.direct_parent,
                                m.kingdom = line.kingdom,
                                m.class = line.class,
                                m.super_class = line.super_class,
                                m.sub_class = line.sub_class,
                                m.chemical_formula = line.chemical_formula,
                                m.average_molecular_weight = line.average_molecular_weight,
                                m.monoisotopic_molecular_weight = line.monoisotopic_molecular_weight,
                                m.chebi_id = line.chebi_id,
                                m.pubchem_compound_id = line.pubchem_compound_id,
                                m.food_id = line.food_id
                    RETURN COUNT(m) AS metabolite_count
                }
                RETURN metabolite_count;
                '''
                result = await tx.run(metabolite_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['metabolite_count']} metabolites")
            except Exception as e:
                logging.error(f"Error loading metabolites: {e}")

            try:
                associated_with_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///hmdb_associated_with_protein.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (m:Metabolite {id: line.START_ID})
                    MATCH (p:Protein {id: line.END_ID})
                    MERGE (m)-[r:ASSOCIATED_WITH]->(p)
                    RETURN COUNT(r) AS metabolite_protein_count
                }
                RETURN metabolite_protein_count;
                '''
                result = await tx.run(associated_with_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['metabolite_protein_count']} ASSOCIATED_WITH relationships")
            except Exception as e:
                logging.error(f"Error loading ASSOCIATED_WITH relationships: {e}")

            try:
                associated_with_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///RESOURCE_associated_with_disease.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (m:Metabolite {id: line.START_ID})
                    MATCH (d:Disease {id: line.END_ID})
                    MERGE (m)-[r:ASSOCIATED_WITH]->(d)
                    RETURN COUNT(r) AS metabolite_disease_count
                }
                RETURN metabolite_disease_count;
                '''
                result = await tx.run(associated_with_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['metabolite_disease_count']} ASSOCIATED_WITH relationships")

            except Exception as e:
                logging.error(f"Error loading ASSOCIATED_WITH relationships: {e}")

            """
            try:
                associated_with_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///RESOURCE_associated_with_tissue.tsv" AS line
                    FIELDTERMINATOR '\t'
                    MATCH (m:Metabolite {id: line.START_ID})
                    MATCH (t:Tissue {id: line.END_ID})
                    MERGE (m)-[r:ASSOCIATED_WITH]->(t)
                    RETURN COUNT(r) AS metabolite_tissue_count
                }
                RETURN metabolite_tissue_count;
                '''
                result = await tx.run(associated_with_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['metabolite_tissue_count']} ASSOCIATED_WITH relationships")
            except Exception as e:
                logging.error(f"Error loading ASSOCIATED_WITH relationships: {e}")
            """

    await driver.close()

async def import_food():

    driver = await get_driver()
    async with driver.session() as session:
        async with session.begin_transaction() as tx:

            try:
                constraint_query = '''
                CREATE CONSTRAINT IF NOT EXISTS FOR (f:Food) REQUIRE f.id IS UNIQUE;
                '''
                await tx.run(constraint_query)
                logging.info("Created constraint for Food")
            except Exception as e:
                logging.error(f"Error creating constraint: {e}")

            try:
                index_query = '''
                CREATE INDEX IF NOT EXISTS FOR (f:Food) ON (f.id);
                CREATE INDEX IF NOT EXISTS FOR (f:Food) ON (f.name);
                CREATE INDEX IF NOT EXISTS FOR (m:Metabolite) ON (m.id);
                '''
                await tx.run(index_query)
                logging.info("Created index for Food id, Food name, and Metabolite id")

            except Exception as e:
                logging.error(f"Error creating index: {e}")

            try:
                food_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Food.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MERGE (f:Food {id: line.ID})
                    ON CREATE SET f.name = line.name,
                                f.scientific_name = line.scientific_name,
                                f.description = line.description,
                                f.group = line.group,
                                f.subgroup = line.subgroup,
                                f.source = line.source
                    RETURN COUNT(f) AS food_count
                }
                RETURN food_count;
                '''
                result = await tx.run(food_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['food_count']} foods")
            except Exception as e:
                logging.error(f"Error loading foods: {e}")

            """
            try:
                food_metabolite_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///_food_has_content.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (f:Food {id: line.START_ID})
                    MATCH (m:Metabolite {id: line.END_ID})
                    MERGE (f)-[r:HAS_CONTENT {
                        minimum: line.min,
                        maximum: line.max,
                        average: line.average,
                        units: line.units,
                        source: line.source
                    }]->(m)
                    RETURN COUNT(r) AS food_content_count
                }
                RETURN food_content_count;
                '''
                result = await tx.run(food_metabolite_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['food_content_count']} HAS_CONTENT relationships")
            except Exception as e:
                logging.error(f"Error loading HAS_CONTENT relationships: {e}")
            """

    await driver.close()

async def import_jensen_lab():

    driver = await get_driver()
    async with driver.session() as session:
        async with session.begin_transaction() as tx:

            try:
                protein_cellular_component_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Protein_Cellular_component_associated_with_integrated.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (p:Protein {id: line.START_ID})
                    MATCH (d:Cellular_component {id: line.END_ID})
                    MERGE (p)-[r:ASSOCIATED_WITH {
                        score: toFloat(line.score),
                        source: line.source,
                        evidence_type: line.evidence_type
                    }]->(d)
                    RETURN COUNT(r) AS jensenlab_association_count
                }
                RETURN jensenlab_association_count;
                '''
                result = await tx.run(protein_cellular_component_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['jensenlab_association_count']} ASSOCIATED_WITH relationships")
            except Exception as e:
                logging.error(f"Error loading ASSOCIATED_WITH relationships: {e}")

            try:
                protein_disease_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Protein_Disease_associated_with_integrated.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (p:Protein {id: line.START_ID})
                    MATCH (d:Disease {id: line.END_ID})
                    MERGE (p)-[r:ASSOCIATED_WITH {
                        score: toFloat(line.score),
                        source: line.source,
                        evidence_type: line.evidence_type
                    }]->(d)
                    RETURN COUNT(r) AS jensenlab_association_count
                }
                RETURN jensenlab_association_count;
                '''
                result = await tx.run(protein_disease_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['jensenlab_association_count']} ASSOCIATED_WITH relationships")
            except Exception as e:
                logging.error(f"Error loading ASSOCIATED_WITH relationships: {e}")

            try:
                protein_tissue_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Protein_Tissue_associated_with_integrated.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (p:Protein {id: line.START_ID})
                    MATCH (t:Tissue {id: line.END_ID})
                    MERGE (p)-[r:ASSOCIATED_WITH {
                        score: toFloat(line.score),
                        source: line.source,
                        evidence_type: line.evidence_type
                    }]->(t)
                    RETURN COUNT(r) AS jensenlab_association_count
                }
                RETURN jensenlab_association_count;
                '''

                result = await tx.run(protein_tissue_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['jensenlab_association_count']} ASSOCIATED_WITH relationships")
            except Exception as e:
                logging.error(f"Error loading ASSOCIATED_WITH relationships: {e}")

    await driver.close()

async def import_publications():

    driver = await get_driver()
    async with driver.session() as session:
        async with session.begin_transaction() as tx:
            try:
                constraint_query = '''
                CREATE CONSTRAINT IF NOT EXISTS FOR (p:Publication) REQUIRE p.id IS UNIQUE;
                '''
                await tx.run(constraint_query)
                logging.info("Created constraint for Publication")
            except Exception as e:
                logging.error(f"Error creating constraint: {e}")

            try:
                index_query = '''
                CREATE INDEX IF NOT EXISTS FOR (p:Publication) ON (p.id);
                CREATE INDEX IF NOT EXISTS FOR (p:Publication) ON (p.title);
                '''
                await tx.run(index_query)
                logging.info("Created index for Publication id and Publication title")

            except Exception as e:
                logging.error(f"Error creating index: {e}")

            try:
                publication_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Publications.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MERGE (p:Publication {id: line.ID})
                    ON CREATE SET p.linkout = line.linkout,
                                p.journal = line.journal_title,
                                p.PMC_id = line.pmcid,
                                p.year = CASE WHEN line.year IS NOT NULL AND line.year <> '' 
                                                THEN toInteger(line.year) 
                                                ELSE null END,
                                p.volume = line.volume,
                                p.issue = line.issue,
                                p.page = line.page,
                                p.DOI = line.doi
                    RETURN COUNT(p) AS publication_count
                }
                RETURN publication_count;
                '''
                result = await tx.run(publication_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['publication_count']} publications")
            except Exception as e:
                logging.error(f"Error loading publications: {e}")

    await driver.close()

async def import_mentions():

    driver = await get_driver()

    async with driver.session() as session:
        async with session.begin_transaction() as tx:

            try:
                mention_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Cellular_component_Publication_mentioned_in_publication.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (p:Publication {id: line.START_ID})
                    MATCH (e:Cellular_component {id: line.END_ID})
                    MERGE (e)-[r:MENTIONED_IN_PUBLICATION]->(p)
                    RETURN COUNT(r) AS mentioned_in_publication_count
                }
                RETURN mentioned_in_publication_count;
                '''
                result = await tx.run(mention_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['mentioned_in_publication_count']} MENTIONED_IN_PUBLICATION relationships")
            except Exception as e:
                logging.error(f"Error loading MENTIONED_IN_PUBLICATION relationships: {e}")

            try:
                mention_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Disease_Publication_mentioned_in_publication.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (p:Publication {id: line.START_ID})
                    MATCH (e:Disease {id: line.END_ID})
                    MERGE (e)-[r:MENTIONED_IN_PUBLICATION]->(p)
                    RETURN COUNT(r) AS mentioned_in_publication_count
                }
                RETURN mentioned_in_publication_count;
                '''
                result = await tx.run(mention_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['mentioned_in_publication_count']} MENTIONED_IN_PUBLICATION relationships")
            except Exception as e:
                logging.error(f"Error loading MENTIONED_IN_PUBLICATION relationships: {e}")

            """
            try:
                mention_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Drug_Publication_mentioned_in_publication.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (p:Publication {id: line.START_ID})
                    MATCH (e:Drug {id: line.END_ID})
                    MERGE (e)-[r:MENTIONED_IN_PUBLICATION]->(p)
                    RETURN COUNT(r) AS mentioned_in_publication_count
                }
                RETURN mentioned_in_publication_count;
                '''
                result = await tx.run(mention_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['mentioned_in_publication_count']} MENTIONED_IN_PUBLICATION relationships")
            except Exception as e:
                logging.error(f"Error loading MENTIONED_IN_PUBLICATION relationships: {e}")
            """
            try:
                mention_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Functional_region_Publication_mentioned_in_publication.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (p:Publication {id: line.START_ID})
                    MATCH (e:Functional_region {id: line.END_ID})
                    MERGE (e)-[r:MENTIONED_IN_PUBLICATION]->(p)
                    RETURN COUNT(r) AS mentioned_in_publication_count
                }
                RETURN mentioned_in_publication_count;
                '''
                result = await tx.run(mention_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['mentioned_in_publication_count']} MENTIONED_IN_PUBLICATION relationships")
            except Exception as e:
                logging.error(f"Error loading MENTIONED_IN_PUBLICATION relationships: {e}")

            try:
                mention_query = '''
                CALL {
                    LOAD CSV WITH HEADERS FROM "file:///Metabolite_Publication_Publication_mentioned_in_publication.tsv" AS line
                    FIELDTERMINATOR '\\t'
                    MATCH (p:Publication {id: line.START_ID})
                    MATCH (e:Metabolite_Publication {id: line.END_ID})
                    MERGE (e)-[r:MENTIONED_IN_PUBLICATION]->(p)
                    RETURN COUNT(r) AS mentioned_in_publication_count
                }
                RETURN mentioned_in_publication_count;
                '''
                result = await tx.run(mention_query)
                records = await result.fetch()
                if records:
                    logging.info(f"Loaded {records[0]['mentioned_in_publication_count']} MENTIONED_IN_PUBLICATION relationships")
            except Exception as e:
                logging.error(f"Error loading MENTIONED_IN_PUBLICATION relationships: {e}")

    await driver.close()                

async def main():
    await import_ontologies()
    # await import_biomarkers()
    # await import_qc_markers()
    await import_chromosomes()
    await import_genes()
    await import_transcripts()
    await import_proteins()
    await import_functional_regions()
    await import_annotations()
    # await import_complexes()
    await import_pathology_expression()
    await import_ppi()
    await import_protein_structure()
    # await import_diseases()
    # await import_drugs()
    # await import side_effects()
    await import_pathways()
    await import_metabolites()
    await import_food()
    await import_jensen_lab()
    await import_publications()
    await import_mentions()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())