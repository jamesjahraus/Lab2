import time
import arcpy
import etl
import logging
import tkinter as tk
from config import config_dict, set_path, setup_logging

logger = logging.getLogger(__name__)


def import_spatial_reference(dataset):
    r"""Extracts the spatial reference from input dataset.

    Arguments:
        dataset: Dataset with desired spatial reference.
    Returns:
        The spatial reference of any dataset input.
    """
    spatial_reference = arcpy.Describe(dataset).spatialReference
    arcpy.AddMessage(f'spatial_reference: {spatial_reference.name}')
    return spatial_reference


def setup_env(workspace_path, spatial_ref_dataset):
    # Set workspace path.
    arcpy.env.workspace = workspace_path
    arcpy.AddMessage('workspace(s): {}'.format(arcpy.env.workspace))

    # Set output overwrite option.
    arcpy.env.overwriteOutput = True
    arcpy.AddMessage('overwriteOutput: {}'.format(arcpy.env.overwriteOutput))

    # Set the output spatial reference.
    arcpy.env.outputCoordinateSystem = import_spatial_reference(
        spatial_ref_dataset)
    arcpy.AddMessage('outputCoordinateSystem: {}'.format(
        arcpy.env.outputCoordinateSystem.name))


def check_status(result):
    r"""Logs the status of executing geoprocessing tools.

    Requires futher investigation to refactor this function:
        I can not find geoprocessing tool name in the result object.
        If the tool name can not be found may need to pass it in.
        Return result.getMessages() needs more thought on what it does.

    Understanding message types and severity:
    https://pro.arcgis.com/en/pro-app/arcpy/geoprocessing_and_python/message-types-and-severity.htm

    Arguments:
        result: An executing geoprocessing tool object.
    Returns:
        Requires futher investigation on what result.getMessages() means on return.
    """
    status_code = dict([(0, 'New'), (1, 'Submitted'), (2, 'Waiting'),
                        (3, 'Executing'), (4, 'Succeeded'), (5, 'Failed'),
                        (6, 'Timed Out'), (7, 'Canceling'), (8, 'Canceled'),
                        (9, 'Deleting'), (10, 'Deleted')])

    arcpy.AddMessage('current job status: {0}-{1}'.format(
        result.status, status_code[result.status]))
    # Wait until the tool completes
    while result.status < 4:
        arcpy.AddMessage('current job status (in while loop): {0}-{1}'.format(
            result.status, status_code[result.status]))
        time.sleep(0.2)
    messages = result.getMessages()
    arcpy.AddMessage('job messages: {0}'.format(messages))
    return messages


def arcgis_setup(flush_output_db=False):
    # Optional flush output_db
    output_db = config_dict.get('output_gdb_dir')
    if flush_output_db:
        arcpy.AddMessage('\nFlushing Output DB')
        arcpy.env.workspace = output_db
        arcpy.AddMessage(f'Contents of Output DB Before Flush {arcpy.ListFeatureClasses()}')
        for fc in arcpy.ListFeatureClasses():
            arcpy.AddMessage(f'Deleting: {fc}')
            if arcpy.Exists(fc):
                arcpy.Delete_management(fc)
        arcpy.AddMessage(f'Contents of Output DB After Flush {arcpy.ListFeatureClasses()}\n')
    # Setup Geoprocessing Environment
    spatial_ref_dataset = config_dict.get('spatial_ref_dataset')
    input_db = config_dict.get('input_gdb_dir')
    setup_env(input_db, spatial_ref_dataset)


def run_etl():
    logger.debug('Starting Etl process.')
    arcpy.AddMessage('Etl process starting...')
    etl_instance = etl.GSheetsEtl(config_dict)
    etl_instance.process()
    logger.debug('Etl process complete.')


def get_map(aprx, map_name):
    for mp in aprx.listMaps():
        if map_name == mp.name:
            arcpy.AddMessage(f'Map called {mp.name} found')
            return mp
    raise ValueError(f'Map called {map_name} does not exist in current aprx {aprx.filePath}')


def buffer(aprx_mp, input_fc, output_fc, lyr_name, buf_distance):
    r"""Run ArcGIS Pro tool Buffer.
    https://pro.arcgis.com/en/pro-app/latest/tool-reference/analysis/buffer.htm

    Arguments:
        aprx_mp: mp object from aprx.listMaps()
        input_fc: Required feature class to buffer.
        output_fc: Output buffered feature class.
        lyr_name: name of layer for the map
        buf_distance: Distance to buffer the feature class.
    Returns:
        arcpy tool result object
    Raises:
        N/A
    """
    logger.debug('Starting Buffer geoprocessing.')
    for lyr in aprx_mp.listLayers():
        if lyr.name == lyr_name:
            arcpy.AddMessage(f'layer {0} already exists, deleting {lyr_name} ...')
            aprx_mp.removeLayer(lyr)
            break
    buf = arcpy.Buffer_analysis(input_fc, output_fc, buf_distance, "FULL",
                                "ROUND", "ALL")
    check_status(buf)
    # lyr = arcpy.MakeFeatureLayer_management(output_fc, lyr_name)
    # aprx_mp.addLayer(lyr[0], 'TOP')
    logger.debug('Buffer geoprocessing complete.')


def intersect(aprx_mp, fc_list, output_fc, lyr_name):
    r"""Run ArcGIS Pro tool Intersect.
    https://pro.arcgis.com/en/pro-app/latest/tool-reference/analysis/intersect.htm

    Arguments:
        fc_list: List of feature classes to run Intersect Analysis on.
        output_fc: Output intersect feature class.
    Returns:
        arcpy tool result object
    Raises:
        N/A
    """
    logger.debug('Starting Intersect geoprocessing.')
    for lyr in aprx_mp.listLayers():
        if lyr.name == lyr_name:
            arcpy.AddMessage(f'layer {lyr_name} already exists, deleting {lyr_name} ...')
            aprx_mp.removeLayer(lyr)
            break
    inter = arcpy.Intersect_analysis(fc_list, output_fc, "ALL")
    check_status(inter)
    # lyr = arcpy.MakeFeatureLayer_management(output_fc, lyr_name)
    # aprx_mp.addLayer(lyr[0], 'TOP')
    logger.debug('Intersect geoprocessing complete.')


def add_feature_to_map(aprx_mp, lyr_name, output_fc, colour):
    logger.debug('Adding feature to map.')
    for lyr in aprx_mp.listLayers():
        if lyr.name == lyr_name:
            arcpy.AddMessage(f'layer {0} already exists, deleting {lyr_name} ...')
            aprx_mp.removeLayer(lyr)
            break
    lyr = arcpy.MakeFeatureLayer_management(output_fc, lyr_name)
    aprx_mp.addLayer(lyr[0], 'TOP')
    for lyr in aprx_mp.listLayers():
        if lyr.name == lyr_name:
            sym = lyr.symbology
            sym.renderer.symbol.color = {'RGB': colour}
            lyr.symbology = sym
    logger.debug('Add feature to map complete.')


def export_map(subtitle):
    logger.debug('Starting map export.')
    # Setup aprx
    aprx_path = set_path(config_dict.get('proj_dir'), 'WestNileOutbreak.aprx')
    aprx = arcpy.mp.ArcGISProject(aprx_path)
    lyt = aprx.listLayouts()[0]
    for el in lyt.listElements():
        arcpy.AddMessage(el.name)
        if 'Title' in el.name:
            el.text = f'{el.text} {subtitle}'
            arcpy.AddMessage(el.text)
    aprx.save()
    lyt.exportToPDF(f'{config_dict["proj_dir"]}/wnv.pdf')
    logger.debug('Export map complete.')


def input_gui():
    logger.debug('Starting input gui.')
    user_inputs = None

    def get_inputs():
        nonlocal user_inputs
        user_inputs = {'intersect_fc': p1.get(), 'buf_distance': p2.get(), 'map_subtitle': p3.get()}

    gui = tk.Tk()
    gui.wm_title('West Nile Virus Simulation Inputs')
    tk.Label(gui, text='Intersect feature class name, example: IntersectAnalysis').grid(sticky=tk.W, row=0)
    p1 = tk.Entry(gui)
    p1.grid(row=0, column=1)
    tk.Label(gui, text='Buffer distance, example: 2500 Feet').grid(sticky=tk.W, row=1)
    p2 = tk.Entry(gui)
    p2.grid(row=1, column=1)
    tk.Label(gui, text='Map subtitle, example: 2500 Feet').grid(sticky=tk.W, row=2)
    p3 = tk.Entry(gui)
    p3.grid(row=2, column=1)
    tk.Button(gui, text='Submit', command=get_inputs).grid(row=3, column=0, sticky=tk.W, pady=4)
    # Quit button hangs while program completes then gui crashes, workaround: close the window after inputs.
    # tk.Button(gui, text='Quit', command=gui.quit).grid(row=3, column=1, sticky=tk.W, pady=4)
    gui.mainloop()

    logger.debug('Input gui complete.')

    return user_inputs


def run_model():
    # setup the logger to generate log file use commands: logger.debug(msg), logger.info(msg)
    setup_logging(level='DEBUG', fn=f'{config_dict["proj_dir"]}/{config_dict["log_fn"]}')

    # Start Input GUI
    user_inputs = input_gui()
    logger.info('Starting West Nile Virus Simulation')
    logger.info(f'Simulation Parameters: {user_inputs}')

    # setup arcpy environment
    output_db = config_dict.get('output_gdb_dir')
    arcpy.AddMessage(f'output db: {output_db}')
    aprx_path = set_path(config_dict.get('proj_dir'), 'WestNileOutbreak.aprx')
    aprx = arcpy.mp.ArcGISProject(aprx_path)
    arcpy.AddMessage(f'aprx path: {aprx.filePath}')
    mp = get_map(aprx, 'Map')

    # Buffer Analysis
    buf_fc_list = ['Mosquito_Larval_Sites', 'Wetlands_Regulatory', 'Lakes_and_Reservoirs', 'OSMP_Properties',
                   'avoid_points']
    for fc in buf_fc_list:
        buf_distance = user_inputs['buf_distance']
        input_fc_name = fc
        buf_fc_name = f'{fc}_buf'
        buf_fc = set_path(output_db, buf_fc_name)
        buffer(mp, input_fc_name, buf_fc, buf_fc_name, buf_distance)
        aprx.save()

    # Intersect Analysis
    # for loop is used to create intersect_fc_list for intersect function (including paths to output_db)
    intersect_fc_list = []
    for fn in buf_fc_list:
        if fn == 'avoid_points':
            arcpy.AddMessage(
                '\nSkipping avoid_points for Intersect Analysis they will be used for Symmetrical Difference.\n')
        else:
            intersect_fn = set_path(output_db, f'{fn}_buf')
            intersect_fc_list.append(intersect_fn)
    intersect_fc_name = user_inputs['intersect_fc']
    inter = set_path(output_db, intersect_fc_name)
    intersect(mp, intersect_fc_list, inter, intersect_fc_name)
    aprx.save()

    # Query by Location
    logger.debug('Starting Spatial Join geoprocessing.')
    join_output_name = 'IntersectAnalysis_Join_BoulderAddresses'
    jofc = set_path(output_db, join_output_name)
    sp = arcpy.SpatialJoin_analysis('Boulder_Addresses', inter, jofc, join_type="KEEP_COMMON", match_option="WITHIN")
    check_status(sp)
    logger.debug('Spatial Join geoprocessing complete.')

    # Record Count
    logger.debug('Starting Get Count geoprocessing.')
    record_count = arcpy.GetCount_management(jofc)
    arcpy.AddMessage(f'\nBoulder Addresses at-risk =  {record_count[0]}\n')
    logger.debug('Get Count geoprocessing complete.')

    # Clip (Analysis)
    # https://pro.arcgis.com/en/pro-app/latest/tool-reference/analysis/clip.htm
    logger.debug('Starting Clip geoprocessing.')
    inFeatures = set_path(output_db, 'avoid_points_buf')
    clipFeatures = set_path(output_db, user_inputs['intersect_fc'])
    clipOutput = set_path(output_db, 'clip_intersect')

    # Execute Clip
    c = arcpy.Clip_analysis(inFeatures, clipFeatures, clipOutput)
    check_status(c)
    logger.debug('Clip geoprocessing complete.')

    # Record re-count
    logger.debug('Starting Spatial Join geoprocessing.')
    join_output_name = 'clip_intersect_Join_BoulderAddresses'
    jofc = set_path(output_db, join_output_name)
    sp = arcpy.SpatialJoin_analysis('Boulder_Addresses', set_path(output_db, 'clip_intersect'), jofc,
                                    join_type="KEEP_COMMON", match_option="WITHIN")
    check_status(sp)
    logger.debug('Spatial Join geoprocessing complete.')
    logger.debug('Starting Get Count geoprocessing.')
    record_count = arcpy.GetCount_management(jofc)
    logger.debug('Get Count geoprocessing complete.')
    arcpy.AddMessage(
        f'\nBoulder Addresses in risk zone that need to be opted out of pesticide spraying =  {record_count[0]}\n')

    # Add desired features to output map and colour the features
    map_features = [(user_inputs['intersect_fc'], [255, 235, 190, 100]),
                    ('avoid_points_buf', [115, 178, 255, 100]),
                    ('clip_intersect_Join_BoulderAddresses', [102, 119, 205, 100])]
    for f, c in map_features:
        fc_name = f
        fc = set_path(output_db, f)
        colour = c
        add_feature_to_map(mp, fc_name, fc, colour)
    aprx.save()

    # Export final map
    export_map(user_inputs['map_subtitle'])
    aprx.save()


if __name__ == '__main__':
    arcgis_setup(flush_output_db=True)
    run_etl()
    run_model()
