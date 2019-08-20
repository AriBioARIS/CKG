import io
import os
import re
import pandas as pd
import numpy as np
import time
from datetime import datetime
import base64
import qrcode
#import barcode
from natsort import natsorted
import flask
import urllib.parse
from urllib.parse import quote as urlquote
from IPython.display import HTML

import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from dash_network import Network

from app import app
from apps import initialApp, projectApp, importsApp, projectCreationApp, dataUploadApp
from apps import projectCreation, dataUpload
from graphdb_builder import builder_utils
from graphdb_builder.builder import loader
from graphdb_builder.experiments import experiments_controller as eh
import ckg_utils
import config.ckg_config as ckg_config

from worker import create_new_project
from graphdb_connector import connector

driver = connector.getGraphDatabaseConnectionConfiguration()
cwd = os.path.abspath(os.path.dirname(__file__))
templateDir = os.path.join(cwd, 'apps/templates')
separator = '|'


app.layout = dcc.Loading(
    children=[html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content', style={'padding-top':10}),
])], style={'text-align':'center','width':'15px', 'height':'15px','margin-top':'-90px','margin-bottom':'-60px','position':'absolute','top':'50%','left':'45%'}, type='cube')

@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname is not None:
        if pathname == '/apps/initial' or pathname == '/':
            return initialApp.layout
        elif pathname.startswith('/apps/projectCreation'):
            projectCreation = projectCreationApp.ProjectCreationApp("Project Creation", "", "", layout = [], logo = None, footer = None)
            return projectCreation.layout
        elif pathname.startswith('/apps/dataUpload'):
            projectId = pathname.split('/')[-1]
            dataUpload = dataUploadApp.DataUploadApp(projectId, "Data Upload", "", "", layout = [], logo = None, footer = None)
            return dataUpload.layout
        elif pathname.startswith('/apps/project'):
            projectId = pathname.split('/')[-1]
            project = projectApp.ProjectApp(projectId, projectId, "", "", layout = [], logo = None, footer = None)
            return project.layout
        elif pathname.startswith('/apps/imports'):
            imports = importsApp.ImportsApp("CKG imports monitoring", "Statistics", "", layout = [], logo = None, footer = None)
            return imports.layout
        else:
            return '404'


###Callbacks for download project
@app.callback(
    Output('download-zip', 'href'),
    [Input('download-zip', 'n_clicks')],[State('url', 'pathname')])
def generate_report_url(n_clicks, pathname):
    project_id = pathname.split('/')[-1]
    return '/downloads/{}'.format(project_id)
    
@app.server.route('/downloads/<value>')
def generate_report_url(value):
    uri = os.path.join(os.getcwd(),"../../data/downloads/"+value+'.zip')
    return flask.send_file(uri, attachment_filename = value+'.zip', as_attachment = True)


###Callbacks for project creation app
def image_formatter(im):
    return f'<img src="data:image/jpeg;base64,{image_base64(im)}">'

def add_internal_identifiers_to_excel(driver, external_id, data):
    subject_ids = projectCreation.get_subjects_in_project(driver, external_id)
    subject_ids = natsorted([item for sublist in subject_ids for item in sublist], reverse=False)
    data.insert(loc=0, column='subject id', value=subject_ids)
    return data

@app.callback(Output('dum-div', 'children'),
             [Input('responsible', 'value'),
              Input('participant', 'value'),
              Input('data-types', 'value'),
              Input('disease', 'value'),
              Input('tissue', 'value'),
              Input('intervention', 'value'),
              Input('number_subjects', 'value'),
              Input('number_timepoints', 'value'),
              Input('upload-data-type', 'value'),
              Input('update_project_id', 'value')])
def update_input(responsible, participant, datatype, timepoints, disease, tissue, intervention, upload_dt, project_id):
    return responsible, participant, datatype, timepoints, disease, tissue, intervention, upload_dt, project_id

@app.callback(Output('responsible', 'value'),
             [Input('add_responsible', 'n_clicks')],
             [State('responsible-picker','value')])
def update_dropdown(n_clicks, value):
    if n_clicks != None:
        return separator.join(value)

@app.callback(Output('participant', 'value'),
             [Input('add_participant', 'n_clicks')],
             [State('participant-picker','value')])
def update_dropdown(n_clicks, value):
    if n_clicks != None:
        return separator.join(value)

@app.callback(Output('data-types', 'value'),
             [Input('add_datatype', 'n_clicks')],
             [State('data-types-picker','value')])
def update_dropdown(n_clicks, value):
    if n_clicks != None:
        return separator.join(value)

@app.callback(Output('disease', 'value'),
             [Input('add_disease', 'n_clicks')],
             [State('disease-picker','value')])
def update_dropdown(n_clicks, value):
    if n_clicks != None:
        return separator.join(value)

@app.callback(Output('tissue', 'value'),
             [Input('add_tissue', 'n_clicks')],
             [State('tissue-picker','value')])
def update_dropdown(n_clicks, value):
    if n_clicks != None:
        return separator.join(value)

@app.callback(Output('intervention', 'value'),
             [Input('add_intervention', 'n_clicks')],
             [State('intervention-picker','value')])
def update_dropdown(n_clicks, value):
    if n_clicks != None:
        return separator.join(value)

@app.callback([Output('project-creation', 'children'),
               Output('update_project_id','children'),
               Output('update_project_id','style')],
              [Input('project_button', 'n_clicks')],
              [State('project name', 'value'),
               State('project acronym', 'value'),
               State('responsible', 'value'),
               State('participant', 'value'),
               State('data-types', 'value'),
               State('number_timepoints', 'value'),
               State('disease', 'value'),
               State('tissue', 'value'),
               State('intervention', 'value'),
               State('number_subjects', 'value'),
               State('project description', 'value'),
               State('date-picker-start', 'date'),
               State('date-picker-end', 'date')])
def create_project(n_clicks, name, acronym, responsible, participant, datatype, timepoints, disease, tissue, intervention, number_subjects, description, start_date, end_date):
    if n_clicks != None and any(elem is None for elem in [name, number_subjects, datatype, disease, tissue, responsible]) == True:
        response = "Insufficient information to create project. Refresh page."
        return response, None, {'display': 'inline-block'}
    if n_clicks != None and any(elem is None for elem in [name, number_subjects, datatype, disease, tissue, responsible]) == False:
        # Get project data from filled-in fields
        projectData = pd.DataFrame([name, acronym, description, number_subjects, datatype, timepoints, disease, tissue, intervention, responsible, participant, start_date, end_date]).T
        projectData.columns = ['name', 'acronym', 'description', 'subjects', 'datatypes', 'timepoints', 'disease', 'tissue', 'intervention', 'responsible', 'participant', 'start_date', 'end_date']
        projectData['status'] = ''
        # Generate project internal identifier bsed on timestamp
        # Excel file is saved in folder with internal id name
        epoch = time.time()
        internal_id = "%s%d" % ("CP", epoch)
        projectData.insert(loc=0, column='internal_id', value=internal_id)
       
        result = create_new_project.apply_async(args=[internal_id, projectData.to_json(), separator], task_id='project_creation_'+internal_id)
        result_output = result.get(timeout=10, propagate=False)
        external_id = list(result_output.keys())[0]

        if result is not None:
            response = "Project successfully submitted. Download Clinical Data template."
        else:
            response = "There was a problem when creating the project."
        return response, '- '+external_id, {'display': 'inline-block'}

@app.callback(Output('download_link', 'href'),
             [Input('download_button', 'n_clicks')],
             [State('update_project_id', 'children')])
def update_download_link(n_clicks, pathname):
  project_id = pathname.split()[-1]
  return '/apps/templates?value=ClinicalData_template_{}.xlsx'.format(project_id)

@app.server.route('/apps/templates')
def serve_static():
    file = flask.request.args.get('value')
    filename = '_'.join(file.split('_')[:-1])+'.xlsx'
    project_id = file.split('_')[-1].split('.')[0]
    df = pd.read_excel('apps/templates/{}'.format(filename))
    df = add_internal_identifiers_to_excel(driver, project_id, df)
    str_io = io.StringIO()
    df.to_csv(str_io, sep='\t', index=False)
    mem = io.BytesIO()
    mem.write(str_io.getvalue().encode('utf-8'))
    mem.seek(0)
    str_io.close()
    return flask.send_file(mem,
                          mimetype='text/csv',
                          attachment_filename='ClinicalData_{}.tsv'.format(project_id),
                          as_attachment=True,
                          cache_timeout=0)

@app.callback(Output('project_button', 'disabled'),
             [Input('project_button', 'n_clicks')])
def disable_submit_button(n_clicks):
    if n_clicks > 0:
        return True


###Callbacks for data upload app
def parse_contents(contents, filename):
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    file = filename.split('.')[-1]
    
    if file == 'txt':
        df = pd.read_csv(io.StringIO(decoded.decode('utf-8')), sep='\t', low_memory=False)
    elif file == 'csv':
        df = pd.read_csv(io.StringIO(decoded.decode('utf-8')), low_memory=False)
    elif file == 'xlsx' or file == 'xls':
        df = pd.read_excel(io.BytesIO(decoded))        
    return df

def export_contents(data, dataDir, filename):
    file = filename.split('.')[-1]
    
    if file == 'txt' or file == 'tsv':
        csv_string = data.to_csv(os.path.join(dataDir, filename), sep='\t', index=False, encoding='utf-8')
    elif file == 'csv':
        csv_string = data.to_csv(os.path.join(dataDir, filename), sep=',', index=False, encoding='utf-8')
    elif file == 'xlsx' or file == 'xls':
        csv_string = data.to_excel(os.path.join(dataDir, filename), index=False, encoding='utf-8')   
    return csv_string

@app.callback(Output('memory-original-data', 'data'),
              [Input('upload-data', 'contents'),
               Input('upload-data', 'filename')])
def store_original_data(contents, filename):
    if contents is not None:
        df = parse_contents(contents, filename)
        return df.to_dict('records')
    else:
        raise PreventUpdate

@app.callback([Output('clinical-table', 'data'),
               Output('clinical-table', 'columns')],
              [Input('memory-original-data', 'data'),
               Input('editing-columns-button', 'n_clicks')],
              [State('clinical-variables-picker', 'value'),
               State('upload-data-type-picker', 'value')])
def update_data(data, n_clicks, variables, dtype):
    if data is None:
        raise PreventUpdate

    columns= []
    df = pd.DataFrame(data, columns=data[0].keys())
    for i in df.columns:
        columns.append({'id': i, 'name': i,
                        'renamable': False, 'deletable': True})
    df = df.to_dict('rows')
    if n_clicks is not None:
        for var in variables:
            columns.append({'id': var, 'name': var,
                            'renamable': False, 'deletable': True})        
    columns = [d for d in columns if d.get('id') != '']
    return df, columns

@app.callback(Output('data-upload', 'children'),
             [Input('submit_button', 'n_clicks')],
             [State('memory-original-data', 'data'),
              State('upload-data', 'filename'),
              State('url', 'pathname'),
              State('upload-data-type-picker', 'value')])
def run_processing(n_clicks, data, filename, path_name, dtype):
    if n_clicks is not None:
        # Get Clinical data from Uploaded and updated table
        df = pd.DataFrame(data, columns=data[0].keys())
        df.fillna(value=pd.np.nan, inplace=True)
        project_id = path_name.split('/')[-1]
        # Extract all relationahips and nodes and save as csv files
        if dtype == 'clinical':
            df = dataUpload.create_new_experiment_in_db(driver, project_id, df, separator=separator)
            loader.partialUpdate(imports=['project', 'experiment']) #This will run loader for clinical only. To run for proteomics, etc, move to after 'else: pass'
        else:
            pass
        # Path to new local folder
        dataDir = '../../data/experiments/PROJECTID/DATATYPE/'.replace('PROJECTID', project_id).replace('DATATYPE', dtype)
        # Check/create folders based on local
        ckg_utils.checkDirectory(dataDir)
        csv_string = export_contents(df, dataDir, filename)
        message = 'FILE successfully uploaded.'.replace('FILE', '"'+filename+'"')
        return message

@app.callback(Output('memory-original-data', 'clear_data'),
              [Input('submit_button', 'n_clicks')])
def clear_click(n_click_clear):
    if n_click_clear is not None and n_click_clear > 0:
        return True
    return False

@app.callback([Output('data_download_link', 'href'),
               Output('data_download_link', 'download')],
              [Input('data_download_button', 'n_clicks')],
              [State('memory-original-data', 'data'),
               State('upload-data-type-picker', 'value')])
def update_table_download_link(n_clicks, data, data_type):
    if n_clicks != None:
        df = pd.DataFrame(data, columns=data[0].keys())
        csv_string = df.to_csv(index=False, encoding='utf-8', sep=';') 
        csv_string = "data:text/csv;charset=utf-8," + urllib.parse.quote(csv_string)
        return csv_string, 'downloaded_DATATYPE_DataUpload.csv'.replace('DATATYPE', data_type)



#############

#         localimagefolder = os.path.join(dataDir, 'QRCodes')


#         # Generate QR code per row and save as png
#         images = []
#         for i, row in clinicalData.iterrows():
#             subject, biosample, ansample = row['subject id'], row['biological_sample id'], row['analytical_sample id']

#             filename = project_id+"_"+subject+"_"+biosample+"_"+ansample+".png"

#             qr = qrcode.QRCode(version=1,
#                                error_correction=qrcode.constants.ERROR_CORRECT_L,
#                                box_size=10,
#                                border=4)
#             qr.add_data(project_id+"_"+subject+"_"+biosample+"_"+ansample)
#             qr.make()
#             img = qr.make_image()
#             imagepath = os.path.join(localimagefolder, project_id+"_"+subject+"_"+biosample+"_"+ansample+".png")
#             img.save(imagepath) # Save image
#             images.append(imagepath)

#         with open(os.path.join(localimagefolder, "output.pdf"), "wb") as f:
#             f.write(img2pdf.convert([i for i in images]))

#         # Add png names as new column in dataframe
#         clinicalData['QR code'] = images



if __name__ == '__main__':
    app.run_server(debug=True, port=5000)
