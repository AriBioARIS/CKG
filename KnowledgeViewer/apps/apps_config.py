

logo = '../static/img/logo.png'

footer = '<div id="PageFooter"><p> The Clinical Knowledge Graph has been implemented by <a href="mailto:alberto.santos@cpr.ku.dk">Alberto Santos</a>, <a>Annelaura B. Nielsen</a> and <a href="mailto:rita.colaco@cpr.ku.dk">Ana R. Colaço</a></p></br> <p>This tool is used by the Clinical Proteomics Department, Prof. Matthias Mann, at <a href="http://www.cpr.ku.dk">Novo Nordisk Foundation Center for Protein Reseach</a></p></div>'

projectPage = {"overview":[
                            ("overview", None, ["basicTable"], {}), 
                            ("number_subjects", None, ["basicTable"], {}), 
                            ("number_analytical_samples", None, ["basicTable"], {})
                            ]
                }
proteomicsPage= {"overview":[
                            ("number_proteins_analytical_sample", None, ["basicBarPlot"], {}),
                            ("number_pepetides_anlytical_sample", None, ["basicBarPlot"], {}),
                            ("number_modified_proteins_analytical_sample", None, ["basicBarPlot"], {})
                            ],
                "stratification":[
                                ("identified_proteins_sample_group", "pca", ["scatterPlot"], {})
                                ],
                    "correlation":[
                                ("correlation_analysis", ["complexHeatmapPlot", "3dNetwork"])
                                ],
                    "regulation":[
                                ("differential_regulation", ["volcanoPlot", "basicTable", "ppiNetwork"])
                                ],
                    "targets":[
                                ("target_analysis_proteins", ["basicTable", "3dNetwork"])
                                ]
                    }
wesPage= {"overview":[
                        ("number_somatic_mutations_by_type_analytical_sample",["basicBarPlot", "basicTable"]),
                    ],
        "targets":[
                        ("target_analysis_variants",["basicTable", "3dNetwork"])
                    ]
        }


### Project Page configuration
pages = {"projectPage":{
                "project": projectPage,
                #"proteomics": proteomicsPage,
                #"wes": wesPage
                }
            }
## Overview 
## Project Name
## Project description
## Studied disease
## Person responsible
## Participants
## Number of enrolled subjects




