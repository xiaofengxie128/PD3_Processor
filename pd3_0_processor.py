import os
import pandas as pd
import logging

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.shortcuts import render
from django.core.files.storage import FileSystemStorage
from django.core.files.uploadedfile import InMemoryUploadedFile

from file_manager.models import SampleRecord, DataAnalysisQueue, ProcessingApp


# app name, must be the same as in the database
APPNAME = "PD3.0 processor"

# folder to store the methods
APPFOLDER = "media/primary_storage/systemfiles/pd3-0/methods/"

# create app folder if not exist
if not os.path.exists(APPFOLDER):
    os.makedirs(APPFOLDER)
logger = logging.getLogger(__name__)

# app view


@ login_required
def view(request):

    args = {
        'SampleRecord':
        SampleRecord.objects.order_by('-pk'),
        'ProcessMethod': [f for f in os.listdir(
            APPFOLDER) if f.endswith('.pdProcessingWF')],
        'ConsensusMethod': [f for f in os.listdir(
            APPFOLDER) if f.endswith('.pdConsensusWF')],
        'QuantifyMethod': [f for f in os.listdir(
            APPFOLDER) if f.endswith('.method')],


    }

# download link for the processor and its configration files
    processor = ProcessingApp.objects.filter(
        name=APPNAME).first().process_package.name
    if processor:
        args['download_processor'] = processor

# selection of the process/consensus/quantify configurations from uploaded or
# existing files. save the uploaded files (process/consensus/quantify) to
# APPFOLDER for future use if save is checked and new method file is uploaded
    if request.method == 'POST':
        # process method
        if (len(request.FILES) != 0 and
                request.POST.get('pd_process_option') == "custom"):
            pd_process_method = request.FILES['pd_process_file']
            if request.POST.get('keep_method') == "True":
                fs = FileSystemStorage(location=APPFOLDER)
                fs.save(pd_process_method.name, pd_process_method)
        elif request.POST.get('pd_process_option') == "None":
            pd_process_method = None
        else:
            process_name = request.POST.get('pd_process_option')
            process_url = APPFOLDER+process_name
            pd_process_method = InMemoryUploadedFile(open(
                process_url, 'r'), None, process_name, None, None, None)

        # consensor method
        if (len(request.FILES) != 0 and
                request.POST.get('pd_consensus_option') == "custom"):
            pd_consensus_method = request.FILES['pd_consensus_file']
            if request.POST.get('keep_method') == "True":
                fs = FileSystemStorage(
                    location=APPFOLDER)
                fs.save(pd_consensus_method.name, pd_consensus_method)
        elif request.POST.get('pd_consensus_option') == "None":
            pd_consensus_method = None
        else:
            consensus_name = request.POST.get('pd_consensus_option')
            consensus_url = APPFOLDER+consensus_name
            pd_consensus_method = InMemoryUploadedFile(open(
                consensus_url, 'r'), None, consensus_name, None, None, None)
        # quantify method
        if (len(request.FILES) != 0 and
                request.POST.get('pd_quantify_option') == "custom"):
            pd_quantify_method = request.FILES['quantify_method_file']
            if request.POST.get('keep_method') == "True":
                fs = FileSystemStorage(
                    location=APPFOLDER)
                fs.save(pd_quantify_method.name, pd_quantify_method)
        elif request.POST.get('pd_quantify_option') == "None":
            pd_quantify_method = None
        else:
            quantify_name = request.POST.get('pd_quantify_option')
            quantify_url = APPFOLDER+quantify_name
            pd_quantify_method = InMemoryUploadedFile(open(
                quantify_url, 'r'), None, quantify_name, None, None, None)

        update_qc = request.POST.get('replace_qc')

        # all the input files
        input_files = {
            "input_file_1": pd_process_method,
            "input_file_2": pd_consensus_method,
            "input_file_3": pd_quantify_method,
        }

        newqueue = {
            "processing_name": request.POST.get('analysis_name'),
            'processing_app': ProcessingApp.objects.filter(
                name=APPNAME).first(),
            'process_creator': request.user,
            "update_qc": update_qc,
        }

        # Attach the valid input files to the queue
        for key, value in input_files.items():
            if value:
                newqueue[key] = value

# crate a data analysis queue, attach the sample records to the queue,
# and update the quanlity check.
        newtask = DataAnalysisQueue.objects.create(**newqueue, )
        for item in request.POST.getlist('rawfile_id'):
            newtask.sample_records.add(
                SampleRecord.objects.filter(pk=item).first())
        if update_qc == "True":
            for item in newtask.sample_records.all():
                SampleRecord.objects.filter(pk=item.pk).update(
                    quanlity_check=newtask.pk)

# render the page
    return render(request,
                  'filemanager/pd3-0_processor.html', args)


def post_processing(queue_id):
    """_this function starts once 3rd party app finished, can be used to
    extract information for the QC numbers, etc._
    """
    analysis_queue = DataAnalysisQueue.objects.filter(pk=queue_id).first()

    # Unique proteins for qc number 1
    if analysis_queue.output_file_1:
        df = pd.read_csv(analysis_queue.output_file_1.path, sep='\t')

        try:
            df = df[df["Master"] == "IsMasterProtein"]
            df = df[df.iloc[:, -1] == "High"]
            df = df[df["Protein FDR Confidence Combined"] == "High"]
            df = df[df["Contaminant"] == False]
            analysis_queue.output_QC_number_1 = len(df.index)
        except KeyError:
            logger.error("output_file_1 key doesn't exist")
            analysis_queue.output_QC_number_1 = 0
    else:
        analysis_queue.output_QC_number_1 = 0

    # Unique peptide for qc number 2
    if analysis_queue.output_file_2:
        df = pd.read_csv(analysis_queue.output_file_2.path, sep='\t')

        try:
            df = df[df["Contaminant"] == False]
            analysis_queue.output_QC_number_2 = len(df.index)
        except KeyError:
            logger.error("output_file_2 key doesn't exist")
            analysis_queue.output_QC_number_2 = 0
    else:
        analysis_queue.output_QC_number_2 = 0

    # Unique psm for qc number 3
    if analysis_queue.output_file_3:
        df = pd.read_csv(analysis_queue.output_file_3.path, sep='\t')

        try:
            analysis_queue.output_QC_number_3 = len(df.index)
        except KeyError:
            logger.error("output_file_3 error")
            analysis_queue.output_QC_number_3 = 0
    else:
        analysis_queue.output_QC_number_3 = 0

    # Unique msms for qc number 4
    if analysis_queue.output_file_4:
        df = pd.read_csv(analysis_queue.output_file_4.path, sep='\t')

        try:
            analysis_queue.output_QC_number_4 = len(df.index)
        except KeyError:
            logger.error("output_file_4 error")
            analysis_queue.output_QC_number_4 = 0
    else:
        analysis_queue.output_QC_number_4 = 0

    analysis_queue.save()
