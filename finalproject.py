import time
import csv
import arcpy
import etl
import logging
import tkinter as tk
from config import config_dict, set_path, setup_logging

logger = logging.getLogger(__name__)


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


def setup_env(workspace_path, spatial_reference):
    r"""Function summary.
    Description sentence(s).
    Arguments:
        arg 1: Description sentence.
        arg 2: Description sentence.
    Returns:
        Description sentence.
    Raises:
        Description sentence.
    """
    # Set workspace path.
    arcpy.env.workspace = workspace_path
    arcpy.AddMessage('workspace(s): {}'.format(arcpy.env.workspace))

    # Set output overwrite option.
    arcpy.env.overwriteOutput = True
    arcpy.AddMessage('overwriteOutput: {}'.format(arcpy.env.overwriteOutput))

    # Set the output spatial reference.
    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(spatial_reference)
    arcpy.AddMessage(f'outputCoordinateSystem: {arcpy.env.outputCoordinateSystem.name}')


def arcgis_setup(flush_output_db=False, spatial_reference=54016):
    r"""Function summary.
    Description sentence(s).
    Arguments:
        arg 1: Description sentence.
        arg 2: Description sentence.
    Returns:
        Description sentence.
    Raises:
        Description sentence.
    """
    # Default spatial reference - Gall stereographic projection
    # https://www.spatialreference.org/ref/esri/54016/
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
    input_db = config_dict.get('input_gdb_dir')
    setup_env(input_db, spatial_reference)


def run_etl():
    r"""Function summary.
    Description sentence(s).
    Arguments:
        arg 1: Description sentence.
        arg 2: Description sentence.
    Returns:
        Description sentence.
    Raises:
        Description sentence.
    """
    logger.debug('Starting Etl process.')
    arcpy.AddMessage('Etl process starting...')
    etl_instance = etl.GSheetsEtl(config_dict)
    etl_instance.process()
    logger.debug('Etl process complete.')


def input_gui():
    r"""Function summary.
    Description sentence(s).
    Arguments:
        arg 1: Description sentence.
        arg 2: Description sentence.
    Returns:
        Description sentence.
    Raises:
        Description sentence.
    """
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


def buffer(input_fc, output_fc, buf_distance):
    r"""Run ArcGIS Pro tool Buffer.
    https://pro.arcgis.com/en/pro-app/latest/tool-reference/analysis/buffer.htm

    Arguments:
        input_fc: Required feature class to buffer.
        output_fc: Output buffered feature class.
        buf_distance: Distance to buffer the feature class.
    Returns:
        Side effect is a buffer fc is output to a db.
    Raises:
        N/A
    """
    logger.debug('Starting Buffer geoprocessing.')
    try:
        result = arcpy.Buffer_analysis(input_fc, output_fc, buf_distance, "FULL", "ROUND", "ALL")
        check_status(result)
    except arcpy.ExecuteError:
        arcpy.AddError(arcpy.GetMessages(2))
    logger.debug('Buffer geoprocessing complete.')


def intersect(fc_list, output_fc):
    r"""Run ArcGIS Pro tool Intersect.
    https://pro.arcgis.com/en/pro-app/latest/tool-reference/analysis/intersect.htm

    Arguments:
        fc_list: List of feature classes to run Intersect Analysis on.
        output_fc: Output intersect feature class.
    Returns:
        Side effect is an intersect fc is output to a db.
    Raises:
        N/A
    """
    logger.debug('Starting Intersect geoprocessing.')
    try:
        result = arcpy.Intersect_analysis(fc_list, output_fc, "ALL")
        check_status(result)
    except arcpy.ExecuteError:
        arcpy.AddError(arcpy.GetMessages(2))
    logger.debug('Intersect geoprocessing complete.')


def erase(input_fc, erase_fc, erase_output):
    r"""Function summary.
    Description sentence(s).
    Arguments:
        arg 1: Description sentence.
        arg 2: Description sentence.
    Returns:
        Description sentence.
    Raises:
        Description sentence.
    """
    # Erase the erase_fc from the input_fc
    # Reference: https://pro.arcgis.com/en/pro-app/latest/tool-reference/analysis/erase.htm
    logger.debug('\nStarting erase')
    try:
        result = arcpy.Erase_analysis(input_fc, erase_fc, erase_output)
        check_status(result)
    except arcpy.ExecuteError:
        arcpy.AddError(arcpy.GetMessages(2))
    logger.debug('erase complete\n')


def spatial_join(target_fc, join_fc, output_fc):
    r"""Function summary.
    Description sentence(s).
    Arguments:
        arg 1: Description sentence.
        arg 2: Description sentence.
    Returns:
        Description sentence.
    Raises:
        Description sentence.
    """
    # Joins the join_fc to the target_fc and creates an output_fc
    # Reference: https://pro.arcgis.com/en/pro-app/latest/tool-reference/analysis/spatial-join.htm
    logger.debug('Starting Spatial Join geoprocessing.')
    try:
        result = arcpy.SpatialJoin_analysis(target_fc, join_fc, output_fc, join_type="KEEP_COMMON",
                                            match_option="WITHIN")
        check_status(result)
    except arcpy.ExecuteError:
        arcpy.AddError(arcpy.GetMessages(2))
    logger.debug('Spatial Join geoprocessing complete.')


def record_count(count_fc):
    r"""Function summary.
    Description sentence(s).
    Arguments:
        arg 1: Description sentence.
        arg 2: Description sentence.
    Returns:
        Description sentence.
    Raises:
        Description sentence.
    """
    # Returns the record count of features in a feature class.
    # Reference: https://pro.arcgis.com/en/pro-app/latest/tool-reference/data-management/get-count.htm
    logger.debug('Starting Get Count geoprocessing.')
    try:
        result = arcpy.GetCount_management(count_fc)
        check_status(result)
        return result[0]
    except arcpy.ExecuteError:
        arcpy.AddError(arcpy.GetMessages(2))
    logger.debug('Get Count geoprocessing complete.')


def get_map(aprx, map_name):
    r"""Function summary.
    Description sentence(s).
    Arguments:
        arg 1: Description sentence.
        arg 2: Description sentence.
    Returns:
        Description sentence.
    Raises:
        Description sentence.
    """
    for mp in aprx.listMaps():
        if map_name == mp.name:
            arcpy.AddMessage(f'Map called {mp.name} found')
            return mp
    raise ValueError(f'Map called {map_name} does not exist in current aprx {aprx.filePath}')


def set_spatial_reference(mp, spatial_reference):
    r"""Function summary.
    Description sentence(s).
    Arguments:
        arg 1: Description sentence.
        arg 2: Description sentence.
    Returns:
        Description sentence.
    Raises:
        Description sentence.
    """
    try:
        # Set spatial reference
        mp.spatialReference = arcpy.SpatialReference(spatial_reference)
    except arcpy.ExecuteError:
        arcpy.AddError(arcpy.GetMessages(2))


def add_feature_to_map(aprx_mp, lyr_name, output_fc, colour, transparency):
    r"""Function summary.
    Description sentence(s).
    Arguments:
        arg 1: Description sentence.
        arg 2: Description sentence.
    Returns:
        Description sentence.
    Raises:
        Description sentence.
    """
    logger.debug('Adding feature to map.')
    arcpy.AddMessage('\nAdding feature to map.')
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
            sym.renderer.symbol.outlineColor = {'RGB': [0, 0, 0, 100]}
            lyr.symbology = sym
            lyr.transparency = transparency
    logger.debug('Add feature to map complete.')


def export_map(aprx, subtitle, address_count):
    r"""Function summary.
    Description sentence(s).
    Arguments:
        arg 1: Description sentence.
        arg 2: Description sentence.
    Returns:
        Description sentence.
    Raises:
        Description sentence.
    """
    logger.debug('Starting map export.')
    lyt = aprx.listLayouts()[0]
    for el in lyt.listElements():
        arcpy.AddMessage(el.name)
        if 'Title' in el.name:
            el.text = f'{el.text} {subtitle}'
            arcpy.AddMessage(el.text)
        elif 'AddressCount' in el.name:
            el.text = f'{el.text} {address_count}'
            arcpy.AddMessage(el.text)
    lyt.exportToPDF(f'{config_dict["proj_dir"]}/wnv.pdf')
    logger.debug('Export map complete.')


def render_layout(map_subtitle, map_features, map_spatial_reference, address_count, output_db):
    r"""Function summary.
    Description sentence(s).
    Arguments:
        arg 1: Description sentence.
        arg 2: Description sentence.
    Returns:
        Description sentence.
    Raises:
        Description sentence.
    """
    # Add desired features to output map and colour the features
    aprx_path = set_path(config_dict.get('proj_dir'), 'WestNileOutbreak.aprx')
    aprx = arcpy.mp.ArcGISProject(aprx_path)
    arcpy.AddMessage(f'aprx path: {aprx.filePath}')
    mp = get_map(aprx, 'Map')
    set_spatial_reference(mp, map_spatial_reference)
    for f, c in map_features:
        fc_name = f
        fc = set_path(output_db, f)
        colour = c
        add_feature_to_map(mp, fc_name, fc, colour, transparency=50)

    # Export final map
    export_map(aprx, map_subtitle, address_count)


def generate_target_addresses_csv(fc):
    r"""Generates target_addresses.csv
    Generates a csv containing all the final target addresses that will require spraying.
    Assumes there is a field called 'FULLADDR' in the input feature class.
    Arguments:
        fc: The target addresses feature class.
    Returns:
        Side effect is target_addresses.csv is created in the WestNileOutbreak project directory.
    """
    # Reference: https://pro.arcgis.com/en/pro-app/latest/arcpy/data-access/searchcursor-class.htm
    try:
        csv_path = f'{config_dict["proj_dir"]}/target_addresses.csv'
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['TargetAddresses']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            fields = ['FULLADDR']
            with arcpy.da.SearchCursor(fc, fields) as cursor:
                for row in cursor:
                    # arcpy.AddMessage(f'Address = {row[0]}')
                    row_dict = {'TargetAddresses': row[0]}
                    # arcpy.AddMessage(f'Writing row to new_addresses.csv: {row_dict}')
                    writer.writerow(row_dict)
    except arcpy.ExecuteError:
        arcpy.AddError(arcpy.GetMessages(2))


def run_analysis(output_db):
    r"""Analysis orchestration.
    Coordinates geospatial analysis operations to create required output feature classes:
     - final_analysis
     - avoid_points_buf
     - Target_Addresses

    No try except block included because these exist in helper functions called by this function.

    Arguments:
        output_db: path of the output data base so each geoprocessing tool can write ouput feature classes.
    Returns:
        Results dictionary with the addresses at risk and map subtitle.
        Side effect is final_analysis, avoid_points_buf, Target_Addresses exist in output_db
    """
    # Start Input GUI
    user_inputs = input_gui()
    logger.info(f'Simulation Parameters: {user_inputs}')

    # Buffer Analysis
    # Create buffers around high risk areas that will require pesticide control spraying.
    # Avoid points is also buffered here for convenience.
    # Avoid points will not be included in the intersect analysis.
    # Avoid points buffer represents individuals that signed up to opt out of pesticide control spraying.
    buf_fc_list = ['Mosquito_Larval_Sites', 'Wetlands_Regulatory', 'Lakes_and_Reservoirs', 'OSMP_Properties',
                   'avoid_points']
    for fc in buf_fc_list:
        buf_distance = user_inputs['buf_distance']
        buf_fc_name = f'{fc}_buf'
        buf_fc = set_path(output_db, buf_fc_name)
        buffer(fc, buf_fc, buf_distance)

    # Intersect Analysis
    # Create an intersect feature layer of all the high risk buffer areas.
    # The intersect feature layer represents the highest risk zone for West Nile Virus transmission.
    # The intersect feature layer includes all areas that will require pesticide control spraying.
    intersect_fc_list = []
    for fn in buf_fc_list:
        if fn == 'avoid_points':
            arcpy.AddMessage('\nSkipping avoid_points not used for Intersect Analysis.\n')
        else:
            intersect_fn = set_path(output_db, f'{fn}_buf')
            intersect_fc_list.append(intersect_fn)
    intersect_fc_name = user_inputs['intersect_fc']
    intersect_fc = set_path(output_db, intersect_fc_name)
    intersect(intersect_fc_list, intersect_fc)

    # Erase the intersection of the intersect layer and the avoid points.
    # Pesticide control spraying needs to occur in the intersect layer.
    # However the city can not spray in the avoid points buffer.
    # Therefore the avoid points will be erased from the intersect layer.
    # The resulting layer will be safe for pesticide control spraying.
    erase_input = intersect_fc
    erase_fc = set_path(output_db, 'avoid_points_buf')
    erase_output = set_path(output_db, 'final_analysis')
    erase(erase_input, erase_fc, erase_output)

    # Perform a spatial join between Boulder_addresses and final_analysis.
    # Then count the addresses in the Target_addresses layer.
    # This count represents the addresses impacted by pesticide spraying.
    # def spatial_join(target_fc, join_fc, output_fc):
    # def record_count(count_fc):
    target_fc = 'Boulder_Addresses'
    join_fc = set_path(output_db, 'final_analysis')
    join_output_fc = set_path(output_db, 'Target_Addresses')
    spatial_join(target_fc, join_fc, join_output_fc)
    addresses_at_risk_count = record_count(join_output_fc)
    arcpy.AddMessage(f'\nBoulder Addresses at-risk =  {addresses_at_risk_count}\n')

    # Create a dictionary of results for external use
    results = {'addresses_at_risk_count': addresses_at_risk_count,
               'map_subtitle': user_inputs['map_subtitle']}
    return results


def main(flush_output_db=False):
    r"""main orchestration
    Coordinates the major operations of the West Nile Outbreak project.
    No try except block included because these exist in helper functions called by this function.
    Arguments:
        flush_output_db=False means contents of the output db will not be deleted.
        flush_output_db=True means contents of the output db will be deleted.
    """
    # Setup geoprocessing environment.
    # NAD 1983 StatePlane Colorado North: https://www.spatialreference.org/ref/esri/102653/
    pcs = 102653
    # Setup the logger to generate log file use commands: logger.debug(msg), logger.info(msg)
    setup_logging(level='DEBUG', fn=f'{config_dict["proj_dir"]}/{config_dict["log_fn"]}')
    # Setup arcgis environment
    arcgis_setup(flush_output_db, spatial_reference=pcs)
    # Setup output db
    output_db = config_dict.get('output_gdb_dir')
    arcpy.AddMessage(f'output db: {output_db}')

    logger.info('Starting West Nile Virus Simulation')

    # ----- run_etl -----
    # Run etl, generates the avoid_points feature class.
    run_etl()

    # ----- run_analysis -----
    # Run Analysis to create the final analysis features.
    analysis_results_dictionary = run_analysis(output_db)

    # ----- render_layout -----
    # Render the map including analysis features, correct colours, subtitle, and addresses at risk count.
    # analysis_results_dictionary = {'map_subtitle': 'debug subtitle', 'addresses_at_risk_count': 123}
    map_features = [('final_analysis', [255, 0, 0, 100]),
                    ('avoid_points_buf', [115, 178, 255, 100]),
                    ('Target_Addresses', [102, 119, 205, 100])]
    map_subtitle = analysis_results_dictionary['map_subtitle']
    map_spatial_reference = pcs
    address_count = analysis_results_dictionary['addresses_at_risk_count']
    render_layout(map_subtitle, map_features, map_spatial_reference, address_count, output_db)

    # ----- generate_report -----
    # Generate a csv report in the WestNileOutbreak directory with the Target Addresses that require spraying.
    target_addresses_fc = set_path(output_db, 'Target_Addresses')
    generate_target_addresses_csv(target_addresses_fc)


if __name__ == '__main__':
    main(flush_output_db=True)
