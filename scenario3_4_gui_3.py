import tkinter as tk
from tkinter import ttk, font, filedialog, messagebox
import os
import sys
import threading
import subprocess
import queue
import logging
from PIL import Image, ImageTk
import tempfile
import io
import shutil
import csv
import uuid
import pickle
import concurrent.futures
from functools import partial
import time # Added: Ensure time module is imported
import random # Added: Ensure random module is imported

import fitz  # PyMuPDF
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload


SCOPES = ['https://www.googleapis.com/auth/drive']
counter_lock = threading.Lock()
processed = 0
uploaded = 0
errors = 0

drive_service_pool = queue.Queue()
rate_limiter = None

CORE_LOGGER_NAME = 'pdf_processor_core_logic'


def get_core_logger():
    return logging.getLogger(CORE_LOGGER_NAME)
# x = [] # This was in the original, assuming it's for debugging or unused. Retaining for now.


class RateLimiter:
    def __init__(self, max_calls_per_min=100):
        self.max_calls = max_calls_per_min
        self.calls_this_window = 0
        self.window_start_time = time.time()
        self.lock = threading.Lock()
        self.logger = get_core_logger()

    def try_acquire(self):
        with self.lock:
            current_time = time.time()
            if current_time - self.window_start_time >= 60:
                self.window_start_time = current_time
                self.calls_this_window = 0

            if self.calls_this_window < self.max_calls:
                self.calls_this_window += 1
                return True
            return False

    def wait_if_needed(self):
        while not self.try_acquire():
            with self.lock:
                wait_duration = (self.window_start_time + 60) - time.time()
            sleep_time = max(0, wait_duration) + random.uniform(0.1, 0.5)
            self.logger.debug(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds.")
            time.sleep(sleep_time)

def get_drive_service(credentials_path):
    logger = get_core_logger()
    creds = None
    token_filename = 'token.pickle'

    base_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    token_path = os.path.join(base_dir, token_filename)

    if os.path.exists(token_path):
        try:
            with open(token_path, 'rb') as token:
                creds = pickle.load(token)
        except Exception as e:
            logger.error(f"Error loading token.pickle from {token_path}: {str(e)}")
            try:
                if os.path.exists(token_path):
                    os.remove(token_path)
                    logger.info(f"Removed potentially corrupted {token_path}.")
            except Exception as ex_remove:
                logger.error(f"Error removing {token_path}: {str(ex_remove)}")
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                logger.info("Refreshing expired credentials...")
                creds.refresh(Request())
                logger.info("Credentials refreshed successfully.")
            except Exception as e:
                logger.error(f"Error refreshing credentials: {str(e)}", exc_info=True)
                creds = None

        if not creds:
            try:
                logger.info("No valid credentials found, initiating new OAuth flow...")
                flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
                creds = flow.run_local_server(port=0)
                logger.info("OAuth flow completed successfully.")
            except Exception as e:
                logger.error(f"Authentication failed: {str(e)}", exc_info=True)
                raise

        try:
            with open(token_path, 'wb') as token:
                pickle.dump(creds, token)
            logger.info(f"Credentials saved to {token_path}")
        except Exception as e:
            logger.error(f"Error saving {token_path}: {str(e)}", exc_info=True)

    return build('drive', 'v3', credentials=creds, cache_discovery=False)


def get_drive_service_from_pool(credentials_path):
    logger = get_core_logger()
    try:
        return drive_service_pool.get_nowait()
    except queue.Empty:
        logger.info("Drive service pool empty, creating new service instance.")
        return get_drive_service(credentials_path)

def return_drive_service_to_pool(service):
    logger = get_core_logger()
    if service:
        try:
            drive_service_pool.put_nowait(service)
        except queue.Full:
            logger.warning("Drive service pool is full. Discarding service instance.")
            pass # Service instance is effectively discarded

def initialize_service_pool(credentials_path, pool_size):
    logger = get_core_logger()
    while not drive_service_pool.empty():
        try:
            drive_service_pool.get_nowait()
        except queue.Empty:
            break

    actual_initialized = 0
    for i in range(pool_size):
        try:
            logger.info(f"Initializing service instance {i+1}/{pool_size} for pool...")
            service = get_drive_service(credentials_path)
            drive_service_pool.put(service)
            actual_initialized +=1
        except Exception as e:
            logger.error(f"Failed to initialize a drive service for pool (instance {i+1}): {str(e)}")
    logger.info(f"Initialized {actual_initialized}/{pool_size} drive service instances in the pool.")


def create_folder_if_not_exists(drive_service, folder_name, parent_folder_id):
    logger = get_core_logger()
    global rate_limiter
    try:
        rate_limiter.wait_if_needed()
        query = f"name='{folder_name}' and '{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
        logger.debug(f"Executing Drive query: {query}")
        response = drive_service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()

        if response.get('files'):
            folder_id = response['files'][0]['id']
            logger.info(f"Folder '{folder_name}' already exists with ID: {folder_id}")
            return folder_id
        else:
            logger.info(f"Folder '{folder_name}' not found, creating it...")
            rate_limiter.wait_if_needed()
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parent_folder_id]
            }
            folder = drive_service.files().create(body=file_metadata, fields='id').execute()
            folder_id = folder.get('id')
            logger.info(f"Folder '{folder_name}' created successfully with ID: {folder_id}")
            return folder_id
    except Exception as e:
        logger.error(f"Folder creation/check error for '{folder_name}': {str(e)}", exc_info=True)
        raise

# Modified function signature to accept drive_file_name
def upload_to_drive(drive_service, file_path, drive_file_name, folder_id, resumable_threshold, chunk_size):
    logger = get_core_logger()
    global rate_limiter
    # drive_file_name is now the name to be used in Google Drive
    # local_file_name is just for logging the source if needed, but drive_file_name is key for upload
    local_file_basename = os.path.basename(file_path) 
    media = None 
    try:
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            logger.error(f"Upload skipped for {drive_file_name} (local: {local_file_basename}): File does not exist or is empty at path {file_path}.")
            raise FileNotFoundError(f"File not found or empty: {file_path}")

        file_size = os.path.getsize(file_path)
        # Use drive_file_name for the 'name' metadata in Google Drive
        file_metadata = {'name': drive_file_name, 'parents': [folder_id]}

        media = MediaFileUpload(
            file_path,
            mimetype='application/pdf',
            resumable=file_size > resumable_threshold,
            chunksize=chunk_size
        )

        rate_limiter.wait_if_needed()
        request = drive_service.files().create(body=file_metadata, media_body=media, fields='id')
        response_body = None

        if not media.resumable():
            logger.info(f"Uploading {drive_file_name} (local: {local_file_basename}, size: {file_size}B) using simple upload.")
            response_body = request.execute(num_retries=1) 
            logger.info(f"File {drive_file_name} uploaded successfully (simple), File ID: {response_body.get('id')}")
        else:
            logger.info(f"Uploading {drive_file_name} (local: {local_file_basename}, size: {file_size}B, chunk: {chunk_size}B) using resumable upload.")
            while response_body is None:
                try:
                    rate_limiter.wait_if_needed()
                    status, response_body = request.next_chunk(num_retries=1)
                    if status:
                        logger.info(f"Resumable Upload {drive_file_name}: {int(status.progress() * 100)}% completed.")
                except Exception as e_chunk_resumable:
                    logger.error(f"Error during resumable chunk upload for {drive_file_name}: {type(e_chunk_resumable).__name__}: {str(e_chunk_resumable)}", exc_info=True)
                    raise 
            logger.info(f"File {drive_file_name} uploaded successfully (resumable), File ID: {response_body.get('id')}")
        
        return response_body.get('id') if response_body else None

    except FileNotFoundError: 
        raise
    except Exception as e:
        logger.error(f"Upload operation failed for {drive_file_name} (local: {local_file_basename}): {type(e).__name__}: {str(e)}", exc_info=True)
        raise 
    finally:
        if media is not None:
            del media


def validate_pdf(file_path):
    logger = get_core_logger()
    try:
        with fitz.open(file_path) as doc: 
            return doc.page_count > 0
    except Exception as e:
        logger.warning(f"PDF validation error for {file_path}: {e}")
        return False

def find_pdf_in_folder(parent_path, filename):
    logger = get_core_logger()
    target_name_base = os.path.splitext(filename)[0].lower()
    for root, _, files in os.walk(parent_path):
        for file in files:
            if os.path.splitext(file)[0].lower() == target_name_base:
                if file.lower().endswith('.pdf'):
                    full_path = os.path.join(root, file)
                    if validate_pdf(full_path):
                        return full_path
                    else:
                        logger.warning(f"Invalid PDF file found and skipped: {full_path}")
    logger.warning(f"PDF file '{filename}' (base: '{target_name_base}') not found in {parent_path} or its subdirectories.")
    return None

def compress_scanned_pdf(input_path, output_path,
                        quality=35,
                        dpi=120,
                        grayscale=True,
                        max_retries=2):
    logger = get_core_logger()
    file_specific_temp_dir = None
    temp_path_img_processing = None 

    for attempt in range(max_retries + 1):
        try:
            if not os.path.exists(input_path):
                raise FileNotFoundError(f"Input file {input_path} not found")

            file_specific_temp_dir = os.path.join(tempfile.gettempdir(), f"pdf_proc_compress_{uuid.uuid4().hex}")
            os.makedirs(file_specific_temp_dir, exist_ok=True)

            temp_path_img_processing = os.path.join(file_specific_temp_dir, f"temp_compressed_{os.path.basename(input_path)}")
            original_size = os.path.getsize(input_path)

            with fitz.open(input_path) as doc, fitz.open() as new_doc: 
                if doc.page_count == 0:
                    logger.warning(f"Input PDF {input_path} has 0 pages. Skipping compression.")
                    return False 

                for page_num, page in enumerate(doc):
                    try:
                        pix = page.get_pixmap(dpi=dpi)
                        img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)

                        if grayscale:
                            img = img.convert('L')

                        img_byte_arr = io.BytesIO()
                        img.save(img_byte_arr, format='JPEG', quality=quality, optimize=True)
                        img_data = img_byte_arr.getvalue()
                        img_byte_arr.close() 

                        new_page_rect = fitz.Rect(0, 0, pix.width, pix.height)
                        new_page = new_doc.new_page(width=new_page_rect.width, height=new_page_rect.height)
                        new_page.insert_image(new_page_rect, stream=img_data)
                    except Exception as page_e:
                        logger.error(f"Error processing page {page_num+1} of {input_path}: {str(page_e)}", exc_info=True)


                if new_doc.page_count == 0 and doc.page_count > 0 :
                     logger.error(f"No pages could be processed into the new PDF for {input_path}.")
                     return False 

                new_doc.save(temp_path_img_processing, garbage=4, deflate=True, clean=True, linear=True)


            if not os.path.exists(temp_path_img_processing) or os.path.getsize(temp_path_img_processing) == 0:
                error_message = f"PyMuPDF failed to save a valid temp_path {temp_path_img_processing} for input {input_path} (attempt {attempt+1})"
                logger.error(error_message)
                raise FileNotFoundError(error_message) 

            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            shutil.move(temp_path_img_processing, output_path) 
            temp_path_img_processing = None 

            new_size = os.path.getsize(output_path)
            ratio = (1 - new_size / original_size) * 100 if original_size > 0 else 0
            logger.info(f"Compression complete for {os.path.basename(input_path)}: {original_size/1024:.1f}KB -> {new_size/1024:.1f}KB (-{ratio:.1f}%)")
            return True 

        except Exception as e:
            logger.error(f"Compression error for {os.path.basename(input_path)} (attempt {attempt+1}/{max_retries + 1}): {str(e)}", exc_info=True)
            if attempt == max_retries:
                logger.error(f"Compression failed for {os.path.basename(input_path)} after {max_retries + 1} attempts.")
                return False
            time.sleep(2 ** attempt + random.uniform(0, 1))
        finally:
            if temp_path_img_processing and os.path.exists(temp_path_img_processing):
                try:
                    os.remove(temp_path_img_processing)
                    logger.debug(f"Cleaned up intermediate compression file: {temp_path_img_processing}")
                except Exception as e_clean_final_temp:
                    logger.warning(f"Final cleanup of intermediate compression file {temp_path_img_processing} failed: {e_clean_final_temp}")
            
            if file_specific_temp_dir and os.path.exists(file_specific_temp_dir):
                 try:
                     shutil.rmtree(file_specific_temp_dir)
                     logger.debug(f"Cleaned up compression temp directory: {file_specific_temp_dir}")
                 except Exception as e_clean_dir:
                     logger.warning(f"Error cleaning compression temp dir {file_specific_temp_dir}: {e_clean_dir}")
    return False 

def process_single_file(row, args, credentials_path, subfolder_id, temp_dir_for_output, ui_queue, total_files_count):
    global processed, uploaded, errors 
    logger = get_core_logger()
    original_name = ""
    base_name = "UNKNOWN_FILE" 
    drive_service = None
    output_pdf_final_temp = None 
    thread_temp_output_base = None 
    upload_success_flag = False 

    try:
        drive_service = get_drive_service_from_pool(credentials_path)
        
        if not row or not row[0]:
            logger.error(f"Invalid row data received: {row}. Skipping.")
            with counter_lock: errors += 1
            return 

        original_name = str(row[0]).strip() 
        if not original_name:
            logger.error(f"Empty original name in row: {row}. Skipping.")
            with counter_lock: errors += 1
            return

        base_name = os.path.splitext(original_name)[0] # This is "179422-21"
        # Desired Google Drive filename will be base_name + ".pdf"
        drive_upload_filename = base_name + ".pdf"


        thread_id = threading.get_ident()
        thread_temp_output_base = os.path.join(temp_dir_for_output, f"thread_outputs_{thread_id}")
        os.makedirs(thread_temp_output_base, exist_ok=True)

        # Local temporary file still includes UUID for local uniqueness
        output_pdf_final_temp = os.path.join(thread_temp_output_base, f"{base_name}_{uuid.uuid4().hex}_compressed.pdf")

        logger.info(f"Processing {base_name} (orig: '{original_name}') -> Local temp: {output_pdf_final_temp}, Drive name: {drive_upload_filename}")
        input_pdf = find_pdf_in_folder(args.source, original_name)

        if not input_pdf:
            with counter_lock: errors += 1
            logger.error(f"Source PDF file not found for: '{original_name}' (searched in {args.source})")
            return 

        compression_success = compress_scanned_pdf(
            input_pdf, output_pdf_final_temp,
            quality=args.quality, dpi=args.dpi, grayscale=not args.no_grayscale, max_retries=2
        )

        if not compression_success:
            with counter_lock: errors += 1
            logger.error(f"Compression ultimately failed for {original_name}. Will not upload.")
            return 

        for upload_attempt in range(args.max_upload_retries):
            try:
                # Pass drive_upload_filename to upload_to_drive
                upload_to_drive(drive_service, output_pdf_final_temp, drive_upload_filename, subfolder_id, args.resumable_threshold, args.chunk_size)
                upload_success_flag = True 
                with counter_lock:
                    processed += 1 
                    uploaded += 1
                logger.info(f"Successfully processed and uploaded: {original_name} as {drive_upload_filename}")
                break 
            except Exception as e_upload_loop:
                str_error = str(e_upload_loop).lower()
                is_ssl_error = "ssl" in str_error or isinstance(e_upload_loop, (ConnectionError, TimeoutError, ConnectionRefusedError, ConnectionAbortedError))
                is_rate_limit = "429" in str_error or "quota" in str_error or "ratelimitexceeded" in str_error or "userratelimitexceeded" in str_error

                base_wait = 2
                if is_rate_limit: base_wait = 15
                elif is_ssl_error: base_wait = 8
                wait_time = base_wait * (2 ** upload_attempt) + random.uniform(1, 5)

                logger.error(
                    f"Upload attempt {upload_attempt + 1}/{args.max_upload_retries} for {drive_upload_filename} (local: {os.path.basename(output_pdf_final_temp)}) failed: {type(e_upload_loop).__name__} - {str(e_upload_loop)}. "
                    f"Waiting {wait_time:.2f}s before retry."
                )
                if upload_attempt == args.max_upload_retries - 1:
                    logger.error(f"Final upload attempt failed for {drive_upload_filename}. Full error details in prior logs.")
                time.sleep(wait_time)
        
        if not upload_success_flag:
            with counter_lock: errors += 1 
            logger.error(f"Failed to upload {drive_upload_filename} after all attempts.")

    except Exception as e_main_proc:
        with counter_lock: errors += 1
        logger.error(f"Unhandled error during processing for '{original_name}' (base: {base_name}): {type(e_main_proc).__name__} - {str(e_main_proc)}", exc_info=True)
        upload_success_flag = False 

    finally:
        if output_pdf_final_temp and os.path.exists(output_pdf_final_temp):
            cleanup_attempts = 5  
            for i in range(cleanup_attempts):
                try:
                    os.remove(output_pdf_final_temp)
                    log_msg_base = "Cleaned up local temporary file"
                    if upload_success_flag: 
                        log_msg = f"{log_msg_base} (after successful upload): {output_pdf_final_temp}"
                    else:
                        log_msg = f"{log_msg_base} (after failed/skipped upload or other error): {output_pdf_final_temp}"
                    logger.info(log_msg)
                    break  
                
                except PermissionError as e_lock:
                    logger.warning(f"Cleanup attempt {i+1}/{cleanup_attempts} for {output_pdf_final_temp} failed (locked): {e_lock}.")
                    if i < cleanup_attempts - 1: 
                        wait_duration = (i + 1) * 0.75 + random.uniform(0.1, 0.5) 
                        logger.info(f"Retrying cleanup in {wait_duration:.2f}s...")
                        time.sleep(wait_duration)
                    else: 
                        logger.error(f"Failed to clean up {output_pdf_final_temp} after {cleanup_attempts} attempts (locked). This may lead to resource issues if it persists for many files.")
                
                except Exception as e_clean_other:
                    logger.error(f"Could not clean up {output_pdf_final_temp} due to an unexpected error during deletion attempt {i+1}: {str(e_clean_other)}", exc_info=True)
                    break 
        elif output_pdf_final_temp: 
            logger.debug(f"Local temporary file {output_pdf_final_temp} not found for cleanup (already deleted or never created).")


        if thread_temp_output_base and os.path.exists(thread_temp_output_base):
            try:
                if not os.listdir(thread_temp_output_base): 
                    os.rmdir(thread_temp_output_base)
                    logger.info(f"Cleaned up empty thread temporary directory: {thread_temp_output_base}")
                else:
                    logger.warning(f"Thread temporary directory {thread_temp_output_base} is not empty. Contains: {os.listdir(thread_temp_output_base)}. Skipping rmdir to preserve locked file.")
            except OSError as e_rm_thread_dir: 
                 logger.warning(f"Could not remove thread temporary directory {thread_temp_output_base} (possibly not empty or permission issue): {e_rm_thread_dir}")
            except Exception as e_rm_thread_dir_other:
                 logger.error(f"Unexpected error removing thread temporary directory {thread_temp_output_base}: {e_rm_thread_dir_other}", exc_info=True)

        if drive_service: 
            return_drive_service_to_pool(drive_service)
            logger.debug(f"Drive service returned to pool by thread for {base_name if base_name else 'unknown file'}.")
        
        files_done_processing = uploaded + errors 
        if total_files_count > 0:
            current_progress_percent = (files_done_processing / total_files_count) * 100
            ui_queue.put(("progress", current_progress_percent))


class QueueHandler(logging.Handler):
    def __init__(self, ui_queue):
        super().__init__()
        self.ui_queue = ui_queue

    def emit(self, record):
        log_entry = self.format(record) 
        self.ui_queue.put(("log", log_entry))

class PDFProcessorGUI:
    def __init__(self, root_window):
        self.root = root_window
        self.root.title("PDF Scan Processing Pipeline")
        self.root.geometry("850x650")
        self.root.minsize(800, 600)

        self.ui_queue = queue.Queue()
        self.total_files_for_progress = 0


        core_logger = logging.getLogger(CORE_LOGGER_NAME)
        core_logger.setLevel(logging.INFO) 
        core_logger.propagate = False 

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

        gui_log_handler = QueueHandler(self.ui_queue)
        gui_log_handler.setFormatter(formatter) 
        core_logger.addHandler(gui_log_handler)

        try:
            core_file_handler = logging.FileHandler('pdf_processor_core.log', encoding='utf-8', mode='a') 
            core_file_handler.setFormatter(formatter)
            core_logger.addHandler(core_file_handler)
        except Exception as e:
            print(f"Error setting up core_file_handler: {e}") 

        self.gui_logger = logging.getLogger('pdf_processor_gui_events')
        self.gui_logger.setLevel(logging.INFO)
        try:
            gui_event_file_handler = logging.FileHandler('pdf_processor_gui_events.log', encoding='utf-8', mode='a') 
            gui_event_file_handler.setFormatter(formatter)
            self.gui_logger.addHandler(gui_event_file_handler)
        except Exception as e:
            print(f"Error setting up gui_event_file_handler: {e}")


        self.notebook = ttk.Notebook(self.root)
        self.welcome_frame = self.create_welcome_frame()
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        self.configure_styles()
        self.root.after(100, self.process_ui_queue) 

    def configure_styles(self):
        style = ttk.Style()
        style.configure("AccentButton.TButton", font=("Arial", 12, "bold"), padding=10)
        style.map("AccentButton.TButton",
            foreground=[('active', 'black'), ('!disabled', 'black')], 
            background=[('active', '#0056b3'), ('!disabled', '#007bff')]
        )
        style.configure("Heading.TLabel", font=("Arial", 16, "bold"), foreground="#333333")
        style.configure("TeamMember.TLabel", font=("Arial", 12), foreground="#555555", padding=5)

    def create_welcome_frame(self):
        welcome_frame = ttk.Frame(self.notebook)
        self.notebook.add(welcome_frame, text="Welcome")

        header_frame = ttk.Frame(welcome_frame)
        header_frame.pack(fill=tk.X, padx=20, pady=20)

        logo_placeholder = ttk.Label(header_frame, text="SCAN PIPELINE", font=("Arial", 24, "bold"), foreground="#06BE1B")
        logo_placeholder.pack(pady=10)

        title_label = ttk.Label(welcome_frame, text="PDF Uploading", style="Heading.TLabel")
        title_label.pack(pady=10)

        team_frame = ttk.LabelFrame(welcome_frame, text="") 
        team_frame.pack(fill=tk.X, padx=100, pady=20)

        member_label = ttk.Label(team_frame, text=f"Developed By AMC Application Support Team", style="TeamMember.TLabel")
        member_label.pack(anchor=tk.W, padx=20, pady=5)

        button_frame = ttk.Frame(welcome_frame)
        button_frame.pack(pady=30)

        enter_button = ttk.Button(button_frame, text="Enter Application", style="AccentButton.TButton", command=self.enter_application)
        enter_button.pack(pady=5, ipadx=20, ipady=5)

        version_label = ttk.Label(welcome_frame, text="@ 2025 Alahly Medical Company", font=("Arial", 8)) 
        version_label.pack(side=tk.BOTTOM, pady=10)

        return welcome_frame

    def enter_application(self):
        if not hasattr(self, 'main_app_frame') or self.main_app_frame is None: 
            self.main_app_frame = self.create_main_app_frame()
            self.notebook.add(self.main_app_frame, text="PDF Processor")
        self.notebook.select(1) 

    def create_main_app_frame(self):
        main_frame = ttk.Frame(self.notebook)

        input_frame = ttk.LabelFrame(main_frame, text="Input Parameters")
        input_frame.pack(fill=tk.X, padx=10, pady=10, expand=False)

        row = 0
        self.csv_path = tk.StringVar()
        ttk.Label(input_frame, text="CSV File:").grid(row=row, column=0, sticky=tk.W, padx=5, pady=5)
        ttk.Entry(input_frame, textvariable=self.csv_path, width=60).grid(row=row, column=1, sticky=tk.W+tk.E, padx=5, pady=5)
        ttk.Button(input_frame, text="Browse...", command=lambda: self.browse_file(self.csv_path, [("CSV files", "*.csv")])).grid(row=row, column=2, padx=5, pady=5)
        row += 1

        self.source_path = tk.StringVar()
        ttk.Label(input_frame, text="Source Folder:").grid(row=row, column=0, sticky=tk.W, padx=5, pady=5)
        ttk.Entry(input_frame, textvariable=self.source_path, width=60).grid(row=row, column=1, sticky=tk.W+tk.E, padx=5, pady=5)
        ttk.Button(input_frame, text="Browse...", command=lambda: self.browse_directory(self.source_path)).grid(row=row, column=2, padx=5, pady=5)
        row += 1

        self.credentials_path = tk.StringVar()
        ttk.Label(input_frame, text="Credentials File:").grid(row=row, column=0, sticky=tk.W, padx=5, pady=5)
        ttk.Entry(input_frame, textvariable=self.credentials_path, width=60).grid(row=row, column=1, sticky=tk.W+tk.E, padx=5, pady=5)
        ttk.Button(input_frame, text="Browse...", command=lambda: self.browse_file(self.credentials_path, [("JSON files", "*.json")])).grid(row=row, column=2, padx=5, pady=5)
        row += 1

        self.parent_folder_id = tk.StringVar()
        ttk.Label(input_frame, text="Parent Folder ID:").grid(row=row, column=0, sticky=tk.W, padx=5, pady=5)
        ttk.Entry(input_frame, textvariable=self.parent_folder_id, width=60).grid(row=row, column=1, sticky=tk.W+tk.E, padx=5, pady=5)
        row += 1

        self.subfolder = tk.StringVar()
        ttk.Label(input_frame, text="Subfolder Name:").grid(row=row, column=0, sticky=tk.W, padx=5, pady=5)
        ttk.Entry(input_frame, textvariable=self.subfolder, width=60).grid(row=row, column=1, sticky=tk.W+tk.E, padx=5, pady=5)
        row += 1
        input_frame.columnconfigure(1, weight=1) 

        advanced_frame_container = ttk.LabelFrame(main_frame, text="Advanced Options")
        advanced_frame_container.pack(fill=tk.X, padx=10, pady=10)

        self.advanced_visible = tk.BooleanVar(value=False)
        self.advanced_content_frame = ttk.Frame(advanced_frame_container) 

        def toggle_advanced_options_visibility():
            if self.advanced_visible.get():
                self.advanced_content_frame.pack(fill=tk.X, padx=5, pady=5)
                self.toggle_advanced_button.configure(text="Hide Advanced Options")
            else:
                self.advanced_content_frame.pack_forget()
                self.toggle_advanced_button.configure(text="Show Advanced Options")

        self.toggle_advanced_button = ttk.Button(
            advanced_frame_container, text="Show Advanced Options",
            command=lambda: (self.advanced_visible.set(not self.advanced_visible.get()), toggle_advanced_options_visibility())
        )
        self.toggle_advanced_button.pack(anchor=tk.W, padx=5, pady=5)

        adv_row = 0
        ttk.Label(self.advanced_content_frame, text="JPEG Quality (1-95):").grid(row=adv_row, column=0, sticky=tk.W, padx=5, pady=2)
        self.quality_var = tk.IntVar(value=60)
        ttk.Scale(self.advanced_content_frame, from_=1, to=95, orient=tk.HORIZONTAL, variable=self.quality_var, length=150).grid(row=adv_row, column=1, sticky=tk.W+tk.E, padx=5, pady=2)
        ttk.Label(self.advanced_content_frame, textvariable=self.quality_var, width=3).grid(row=adv_row, column=2, padx=5, pady=2)
        adv_row += 1

        ttk.Label(self.advanced_content_frame, text="Image DPI (72-300):").grid(row=adv_row, column=0, sticky=tk.W, padx=5, pady=2)
        self.dpi_var = tk.IntVar(value=150)
        ttk.Scale(self.advanced_content_frame, from_=72, to=300, orient=tk.HORIZONTAL, variable=self.dpi_var, length=150).grid(row=adv_row, column=1, sticky=tk.W+tk.E, padx=5, pady=2)
        ttk.Label(self.advanced_content_frame, textvariable=self.dpi_var, width=3).grid(row=adv_row, column=2, padx=5, pady=2)
        adv_row += 1

        self.no_grayscale = tk.BooleanVar(value=False) 
        ttk.Checkbutton(self.advanced_content_frame, text="Keep Color (Larger Files)", variable=self.no_grayscale).grid(row=adv_row, column=0, columnspan=2, sticky=tk.W, padx=5, pady=2)
        adv_row += 1

        cpu_cores = os.cpu_count() or 1
        default_workers = min(max(1, cpu_cores // 2 if cpu_cores > 1 else 1), 8) 
        self.max_workers = tk.IntVar(value=default_workers)
        ttk.Label(self.advanced_content_frame, text="Max Parallel Workers:").grid(row=adv_row, column=0, sticky=tk.W, padx=5, pady=2)
        ttk.Spinbox(self.advanced_content_frame, from_=1, to=max(16, cpu_cores*2), textvariable=self.max_workers, width=5).grid(row=adv_row, column=1, sticky=tk.W, padx=5, pady=2)
        adv_row += 1

        self.rate_limit_var = tk.IntVar(value=300) 
        ttk.Label(self.advanced_content_frame, text="API Rate Limit (calls/min):").grid(row=adv_row, column=0, sticky=tk.W, padx=5, pady=2)
        ttk.Spinbox(self.advanced_content_frame, from_=30, to=1000, increment=30, textvariable=self.rate_limit_var, width=7).grid(row=adv_row, column=1, sticky=tk.W, padx=5, pady=2)
        adv_row+=1

        self.advanced_content_frame.columnconfigure(1, weight=1)
        toggle_advanced_options_visibility() 

        progress_frame = ttk.LabelFrame(main_frame, text="Progress & Logs")
        progress_frame.pack(fill=tk.BOTH, padx=10, pady=10, expand=True)

        self.log_text = tk.Text(progress_frame, height=10, wrap=tk.WORD, state=tk.DISABLED, relief=tk.SUNKEN, borderwidth=1)
        log_scroll = ttk.Scrollbar(progress_frame, orient=tk.VERTICAL, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=log_scroll.set)
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(5,0), pady=5)
        log_scroll.pack(side=tk.RIGHT, fill=tk.Y, padx=(0,5), pady=5)

        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(main_frame, orient=tk.HORIZONTAL, length=100, mode='determinate', variable=self.progress_var)
        self.progress_bar.pack(fill=tk.X, padx=10, pady=(0,5), expand=False)

        self.status_var = tk.StringVar(value="Ready")
        status_label = ttk.Label(main_frame, textvariable=self.status_var, anchor=tk.W)
        status_label.pack(fill=tk.X, padx=10, pady=(0,10))

        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, padx=10, pady=5)

        self.start_button = ttk.Button(button_frame, text="Start Processing", command=self.start_processing, style="AccentButton.TButton")
        self.start_button.pack(side=tk.RIGHT, padx=5)

        self.stop_button = ttk.Button(button_frame, text="Stop", command=self.stop_processing, state=tk.DISABLED) 
        self.stop_button.pack(side=tk.RIGHT, padx=5)

        help_button = ttk.Button(button_frame, text="Help", command=self.show_help)
        help_button.pack(side=tk.LEFT, padx=5)

        return main_frame

    def browse_file(self, string_var, filetypes):
        filename = filedialog.askopenfilename(title="Select a file", filetypes=filetypes)
        if filename: string_var.set(filename)

    def browse_directory(self, string_var):
        directory = filedialog.askdirectory(title="Select a directory")
        if directory: string_var.set(directory)

    def validate_inputs(self):
        required = {
            "CSV File": self.csv_path.get(), "Source Folder": self.source_path.get(),
            "Credentials File": self.credentials_path.get(), "Parent Folder ID": self.parent_folder_id.get(),
            "Subfolder Name": self.subfolder.get()
        }
        missing = [name for name, val in required.items() if not val.strip()]
        if missing:
            messagebox.showerror("Missing Information", "Please provide:\n• " + "\n• ".join(missing))
            return False

        if not os.path.isfile(self.csv_path.get()):
            messagebox.showerror("File Error", f"CSV file not found: {self.csv_path.get()}")
            return False
        if not os.path.isdir(self.source_path.get()):
            messagebox.showerror("Folder Error", f"Source folder not found: {self.source_path.get()}")
            return False
        if not os.path.isfile(self.credentials_path.get()):
            messagebox.showerror("File Error", f"Credentials file not found: {self.credentials_path.get()}")
            return False
        return True

    def start_processing(self):
        if not self.validate_inputs():
            return

        self.start_button.configure(state=tk.DISABLED)
        self.stop_button.configure(state=tk.NORMAL) 
        self.status_var.set("Initializing...")
        self.progress_var.set(0)
        self.total_files_for_progress = 0 

        self.log_text.configure(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.configure(state=tk.DISABLED)

        self.ui_queue.put(("log", "--- Starting New Processing Run ---"))

        class ArgsNamespace: pass 
        args = ArgsNamespace()
        args.csv = self.csv_path.get()
        args.source = self.source_path.get()
        args.credentials = self.credentials_path.get()
        args.parent_folder_id = self.parent_folder_id.get()
        args.subfolder = self.subfolder.get()
        args.quality = self.quality_var.get()
        args.dpi = self.dpi_var.get()
        args.max_workers = self.max_workers.get()
        args.no_grayscale = self.no_grayscale.get()
        args.service_pool_size = args.max_workers 
        args.max_upload_retries = 5
        args.resumable_threshold = 5 * 1024 * 1024
        args.chunk_size = 2 * 1024 * 1024
        args.rate_limit = self.rate_limit_var.get()
        args.batch_size = 200 
        args.temp_dir = None 

        param_log = "Processing Parameters:\n"
        for k, v in vars(args).items():
            param_log += f"  - {k}: {v}\n"
        self.ui_queue.put(("log", param_log.strip()))


        self.processing_thread = threading.Thread(
            target=self.run_core_processing_logic_thread_entry,
            args=(args,), daemon=True 
        )
        self.processing_thread.start()

    def run_core_processing_logic_thread_entry(self, args_obj):
        global processed, uploaded, errors, rate_limiter 

        processed = 0
        uploaded = 0
        errors = 0

        while not drive_service_pool.empty():
            try:
                drive_service_pool.get_nowait()
            except queue.Empty:
                break

        current_core_logger = get_core_logger() 

        try:
            execute_pdf_processing_main(args_obj, self.ui_queue, current_core_logger)
            final_status_msg = f"Processing completed. Uploaded: {uploaded}, Errors: {errors}."
            self.ui_queue.put(("status", final_status_msg))
            self.ui_queue.put(("log", final_status_msg))

        except Exception as e:
            current_core_logger.error(f"Core processing logic failed critically: {str(e)}", exc_info=True)
            self.ui_queue.put(("status", f"Processing CRITICALLY failed: {str(e)}"))
            self.ui_queue.put(("log", f"CRITICAL ERROR: {str(e)}"))
        finally:
            self.ui_queue.put(("buttons", "reset"))
            self.ui_queue.put(("log", "--- Processing Run Finished ---"))


    def log_to_ui_textarea(self, message):
        if not isinstance(message, str): 
            message = str(message)
        self.log_text.configure(state=tk.NORMAL)
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END) 
        self.log_text.configure(state=tk.DISABLED)

    def process_ui_queue(self):
        try:
            while True: 
                message_type, message_content = self.ui_queue.get_nowait()

                if message_type == "log":
                    self.log_to_ui_textarea(message_content)
                elif message_type == "progress":
                    self.progress_var.set(float(message_content)) 
                    if self.total_files_for_progress > 0 :
                         files_done = int((float(message_content) / 100.0) * self.total_files_for_progress)
                         self.status_var.set(f"Processing... {files_done}/{self.total_files_for_progress} ({float(message_content):.1f}%)")
                    else:
                         self.status_var.set(f"Processing... ({float(message_content):.1f}%)")
                elif message_type == "status":
                    self.status_var.set(str(message_content)) 
                elif message_type == "total_files":
                    self.total_files_for_progress = int(message_content) 
                    self.status_var.set(f"Preparing to process {self.total_files_for_progress} files...")
                elif message_type == "buttons":
                    if message_content == "reset":
                        self.start_button.configure(state=tk.NORMAL)
                        self.stop_button.configure(state=tk.DISABLED)

                self.ui_queue.task_done() 

        except queue.Empty: 
            pass
        finally:
            self.root.after(100, self.process_ui_queue) 

    def stop_processing(self):
        self.ui_queue.put(("log", "Stop request received (placeholder - may not stop active file processing immediately)."))
        self.gui_logger.info("Stop processing requested by user (placeholder).")
        messagebox.showinfo("Stop Processing", "Stop functionality is a placeholder. Active operations on the current file may continue. New file processing will be halted if implemented via a check flag.")


    def show_help(self):
        help_text = """
PDF Processing Application Help

This application compresses PDF files from a CSV list and uploads them to a specified Google Drive folder.

Required Inputs:
- CSV File: Path to the CSV file. The first column should contain PDF filenames (with or without .pdf extension).
- Source Folder: The local parent directory where the original PDF files (or their subfolders) are located.
- Credentials File: Path to your Google Cloud OAuth 2.0 client_secret.json file.
- Parent Folder ID: The ID of the Google Drive folder where the subfolder for uploads will be created.
- Subfolder Name: The name of the subfolder to be created (or used if existing) in the Parent Folder for uploads.

Advanced Options:
- JPEG Quality: Image compression quality (1-95, lower means smaller files but lower quality). Default: 60.
- Image DPI: Dots Per Inch for rendering PDF pages to images before compression (72-300). Lower DPI means smaller files. Default: 150.
- Keep Color: If checked, PDFs are processed in color. If unchecked (default), images are converted to grayscale, resulting in smaller files.
- Max Parallel Workers: Number of files to process concurrently. Default is based on CPU cores.
- API Rate Limit: Approximate maximum Google Drive API calls per minute. Default: 300.

Workflow:
1. Fill in all required parameters.
2. Adjust advanced options if needed.
3. Click "Start Processing".
4. The application will find PDFs, compress them, and upload them to Google Drive.
5. Progress and logs will be displayed in the main window.
6. Upon first run or if credentials expire, a web browser may open for Google authentication.

Log Files:
- pdf_processor_core.log: Detailed logs of the PDF processing and uploading operations.
- pdf_processor_gui_events.log: Logs related to GUI actions.

Contact the development team for further assistance if needed.
"""
        messagebox.showinfo("Help & Instructions", help_text)

def execute_pdf_processing_main(args_ns, ui_q, core_log_instance): 
    global rate_limiter, processed, uploaded, errors 

    core_log_instance.info("Core processing logic started.")

    if not (1 <= args_ns.quality <= 95):
        core_log_instance.error("Quality must be between 1 and 95.")
        raise ValueError("Quality must be between 1 and 95.")
    if args_ns.dpi < 72:
        core_log_instance.warning(f"DPI set to {args_ns.dpi}, which is low. Recommended >= 72.")

    rate_limiter = RateLimiter(max_calls_per_min=args_ns.rate_limit)
    core_log_instance.info(f"Rate limiter configured to {args_ns.rate_limit} calls/min.")

    user_specified_temp_dir_base = bool(args_ns.temp_dir)
    temp_dir_for_output_base = ""
    if user_specified_temp_dir_base:
        temp_dir_for_output_base = args_ns.temp_dir
    else:
        temp_dir_for_output_base = tempfile.mkdtemp(prefix=f"pdf_main_outputs_{uuid.uuid4().hex[:6]}_")

    os.makedirs(temp_dir_for_output_base, exist_ok=True)
    core_log_instance.info(f"Using base directory for temporary outputs: {temp_dir_for_output_base}")

    if args_ns.service_pool_size < args_ns.max_workers:
        core_log_instance.warning(f"Service pool size ({args_ns.service_pool_size}) is less than max workers ({args_ns.max_workers}). Adjusting pool size to {args_ns.max_workers}.")
        args_ns.service_pool_size = args_ns.max_workers

    core_log_instance.info(f"Initializing Google Drive service connection pool with {args_ns.service_pool_size} connections")
    initialize_service_pool(args_ns.credentials, pool_size=args_ns.service_pool_size)

    initial_drive_service = None
    try:
        initial_drive_service = get_drive_service_from_pool(args_ns.credentials) 
        core_log_instance.info("Connected to Google Drive successfully for initial setup.")
    except Exception as e:
        core_log_instance.error(f"Google Drive connection failed for initial setup: {str(e)}", exc_info=True)
        if not user_specified_temp_dir_base and os.path.exists(temp_dir_for_output_base): shutil.rmtree(temp_dir_for_output_base)
        raise
    finally:
        if initial_drive_service: 
            return_drive_service_to_pool(initial_drive_service)


    subfolder_id = None
    drive_service_for_folder_creation = None
    try:
        drive_service_for_folder_creation = get_drive_service_from_pool(args_ns.credentials)
        subfolder_id = create_folder_if_not_exists(drive_service_for_folder_creation, args_ns.subfolder, args_ns.parent_folder_id)
        core_log_instance.info(f"Using Google Drive destination folder '{args_ns.subfolder}' with ID: {subfolder_id}")
    except Exception as e:
        core_log_instance.error(f"Google Drive folder creation/access failed: {str(e)}", exc_info=True)
        if not user_specified_temp_dir_base and os.path.exists(temp_dir_for_output_base): shutil.rmtree(temp_dir_for_output_base)
        raise
    finally:
        if drive_service_for_folder_creation:
            return_drive_service_to_pool(drive_service_for_folder_creation)


    all_files_from_csv = []
    try:
        with open(args_ns.csv, 'r', newline='', encoding='utf-8-sig') as csvfile: 
            reader = csv.reader(csvfile)
            try:
                header = next(reader)
                core_log_instance.info(f"CSV Header: {header}")
            except StopIteration:
                core_log_instance.error("CSV file is empty or has no header!")
                if not user_specified_temp_dir_base and os.path.exists(temp_dir_for_output_base): shutil.rmtree(temp_dir_for_output_base)
                raise ValueError("CSV file is empty or has no header.")

            for row_num, row_data in enumerate(reader): 
                if row_data and row_data[0] and str(row_data[0]).strip(): 
                    all_files_from_csv.append((row_data, row_num + 2)) 
                else:
                    core_log_instance.warning(f"Skipping empty or invalid row in CSV at line {row_num + 2}")

            if not all_files_from_csv:
                core_log_instance.info("No valid files listed in the CSV after the header.")
                ui_q.put(("status", "No files to process from CSV."))
                if not user_specified_temp_dir_base and os.path.exists(temp_dir_for_output_base): shutil.rmtree(temp_dir_for_output_base)
                return

            core_log_instance.info(f"Found {len(all_files_from_csv)} files to process from CSV.")
            ui_q.put(("total_files", len(all_files_from_csv))) 

    except FileNotFoundError:
        core_log_instance.error(f"CSV file not found: {args_ns.csv}")
        if not user_specified_temp_dir_base and os.path.exists(temp_dir_for_output_base): shutil.rmtree(temp_dir_for_output_base)
        raise
    except Exception as e_csv:
        core_log_instance.error(f"Error reading CSV file {args_ns.csv}: {str(e_csv)}", exc_info=True)
        if not user_specified_temp_dir_base and os.path.exists(temp_dir_for_output_base): shutil.rmtree(temp_dir_for_output_base)
        raise

    try:
        total_files_to_process = len(all_files_from_csv)
        batch_size = min(args_ns.batch_size, total_files_to_process) if total_files_to_process > 0 else 1
        num_batches = (total_files_to_process + batch_size - 1) // batch_size if total_files_to_process > 0 else 0

        core_log_instance.info(f"Processing {total_files_to_process} files in {num_batches} batches of up to {batch_size} files each using {args_ns.max_workers} workers.")
        ui_q.put(("status", f"Starting processing of {total_files_to_process} files..."))

        for batch_num in range(num_batches):
            start_idx = batch_num * batch_size
            end_idx = min((batch_num + 1) * batch_size, total_files_to_process)
            current_batch_items = all_files_from_csv[start_idx:end_idx]

            batch_info_msg = f"--- Starting Batch {batch_num+1}/{num_batches} ({len(current_batch_items)} files) ---"
            core_log_instance.info(batch_info_msg)
            ui_q.put(("log", batch_info_msg))

            with concurrent.futures.ThreadPoolExecutor(max_workers=args_ns.max_workers) as executor:
                process_file_partial = partial(process_single_file, 
                                      args=args_ns,
                                      credentials_path=args_ns.credentials,
                                      subfolder_id=subfolder_id,
                                      temp_dir_for_output=temp_dir_for_output_base,
                                      ui_queue=ui_q,
                                      total_files_count=total_files_to_process
                                      )
                future_to_row_info = {executor.submit(process_file_partial, item[0]): item for item in current_batch_items}

                for future in concurrent.futures.as_completed(future_to_row_info):
                    row_info_tuple = future_to_row_info[future]
                    original_row_content, original_csv_line_num = row_info_tuple
                    try:
                        future.result() 
                    except Exception as e_future:
                        filename_in_row = original_row_content[0] if original_row_content and original_row_content[0] else "UNKNOWN_ROW_DATA"
                        core_log_instance.error(f"Major error propagated from thread for CSV line {original_csv_line_num} (file: {filename_in_row}): {type(e_future).__name__} - {str(e_future)}", exc_info=True)

            batch_completed_msg = f"--- Completed Batch {batch_num+1}/{num_batches} ---"
            stats_msg = f"Current Stats: Uploaded={uploaded}, Errors={errors}"
            core_log_instance.info(batch_completed_msg)
            core_log_instance.info(stats_msg)
            ui_q.put(("log", batch_completed_msg))
            ui_q.put(("log", stats_msg))

            if batch_num < num_batches - 1: 
                estimated_calls_this_batch = len(current_batch_items) * 3 
                estimated_api_time_sec = (estimated_calls_this_batch / args_ns.rate_limit) * 60 if args_ns.rate_limit > 0 else 30
                pause_duration = max(5, estimated_api_time_sec * 0.10) 
                pause_msg = f"Short pause between batches for {pause_duration:.2f} seconds..."
                core_log_instance.info(pause_msg)
                ui_q.put(("log", pause_msg))
                time.sleep(pause_duration)

    finally: 
        if not user_specified_temp_dir_base and os.path.exists(temp_dir_for_output_base):
            try:
                shutil.rmtree(temp_dir_for_output_base)
                core_log_instance.info(f"Cleaned up main temporary output directory: {temp_dir_for_output_base}")
            except Exception as e_clean_outer:
                core_log_instance.warning(f"Failed to clean up main temporary output directory {temp_dir_for_output_base}. It might contain uncleaned thread-specific temp files: {str(e_clean_outer)}")
        elif user_specified_temp_dir_base:
            core_log_instance.info(f"Temporary output files (if any remain) are in user-specified base directory: {temp_dir_for_output_base}")

    summary_header = "\n======= FINAL PROCESSING SUMMARY ======="
    summary_total = f"- Total files from CSV (intended for processing): {len(all_files_from_csv)}"
    summary_uploaded = f"- Files successfully processed and uploaded: {uploaded}"
    summary_errors = f"- Total files resulting in errors (compression or upload): {errors}"
    summary_footer = "========================================"

    core_log_instance.info(summary_header)
    core_log_instance.info(summary_total)
    core_log_instance.info(summary_uploaded)
    core_log_instance.info(summary_errors)
    core_log_instance.info(summary_footer)

    ui_q.put(("log", summary_header))
    ui_q.put(("log", summary_total))
    ui_q.put(("log", summary_uploaded))
    ui_q.put(("log", summary_errors))
    ui_q.put(("log", summary_footer))

    core_log_instance.info("Core processing logic finished.")


def main_gui(): 
    root = tk.Tk()
    app = PDFProcessorGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main_gui()
