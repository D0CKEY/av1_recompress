"""
HTTP Server Mixin for AV1 Batch Encoder
Provides a full-featured web interface that mirrors the desktop GUI.
"""
import threading
import time
import json
import os
from pathlib import Path
from urllib.parse import parse_qs, urlparse

try:
    from .i18n import is_status_completed, parse_size_to_bytes, format_size_auto, t, TRANSLATIONS, CURRENT_LANGUAGE
except ImportError:
    # Fallback if imports fail
    def is_status_completed(status):
        return status and ('[OK]' in str(status) or 'Done' in str(status) or 'Completed' in str(status))
    def parse_size_to_bytes(size_str):
        return None
    def format_size_auto(size_bytes):
        return f"{size_bytes / (1024*1024*1024):.2f} GB"
    def t(key):
        return key
    TRANSLATIONS = {'hu': {}, 'en': {}}
    CURRENT_LANGUAGE = 'hu'

try:
    from http.server import BaseHTTPRequestHandler, HTTPServer
except ImportError:
    BaseHTTPRequestHandler = object
    HTTPServer = None

try:
    from flask import Flask, jsonify, request, Response
    FLASK_AVAILABLE = True
except ImportError:
    FLASK_AVAILABLE = False


class HttpServerMixin:
    """Mixin class providing HTTP server functionality for the GUI."""
    
    def start_http_server(self, port=5000):
        """Start the HTTP server on the specified port."""
        self.http_port = port
        
        # Initialize HTTP-specific language standalone from GUI
        if not hasattr(self, 'http_language'):
            self.http_language = CURRENT_LANGUAGE
            
        if FLASK_AVAILABLE:
            self._start_flask_server(port)
        else:
            print("Flask not found, falling back to standard library HTTP server.")
            self._start_std_server(port)

    def _t(self, key):
        """HTTP specific translation helper."""
        lang = getattr(self, 'http_language', 'hu')
        # Fallback logic: Try requested lang -> Try English -> Return key
        return TRANSLATIONS.get(lang, {}).get(key, TRANSLATIONS.get('en', {}).get(key, key))

    def _start_flask_server(self, port):
        """Start Flask-based HTTP server."""
        self.flask_app = Flask('av1_recompress.gui')
        self.flask_app.secret_key = 'av1_recompress_secret'

        # Enable debug mode for better error messages
        self.flask_app.config['DEBUG'] = True
        self.flask_app.config['PROPAGATE_EXCEPTIONS'] = True

        # Routes with error handling
        @self.flask_app.route('/')
        def index():
            try:
                return self._http_index()
            except Exception as e:
                import traceback
                error_msg = f"Error in index route: {e}\n{traceback.format_exc()}"
                print(error_msg)
                return f"<h1>Internal Server Error</h1><pre>{error_msg}</pre>", 500
        @self.flask_app.route('/api/status')
        def status():
            try:
                return jsonify(self._get_status_data())
            except Exception as e:
                import traceback
                print(f"Error in status route: {e}\n{traceback.format_exc()}")
                return jsonify({'error': str(e)}), 500

        @self.flask_app.route('/api/videos')
        def videos():
            try:
                return self._api_get_videos()
            except Exception as e:
                import traceback
                print(f"Error in videos route: {e}\n{traceback.format_exc()}")
                return jsonify({'error': str(e)}), 500

        @self.flask_app.route('/api/settings', methods=['GET'])
        def get_settings():
            try:
                return jsonify(self._get_settings_data())
            except Exception as e:
                import traceback
                print(f"Error in settings route: {e}\n{traceback.format_exc()}")
                return jsonify({'error': str(e)}), 500

        @self.flask_app.route('/api/settings', methods=['POST'])
        def set_settings():
            try:
                return self._api_set_settings()
            except Exception as e:
                import traceback
                print(f"Error in set_settings route: {e}\n{traceback.format_exc()}")
                return jsonify({'error': str(e)}), 500

        @self.flask_app.route('/api/action/start', methods=['POST'])
        def start():
            try:
                return self._api_start()
            except Exception as e:
                import traceback
                print(f"Error in start route: {e}\n{traceback.format_exc()}")
                return jsonify({'error': str(e)}), 500

        @self.flask_app.route('/api/action/stop', methods=['POST'])
        def stop():
            try:
                return self._api_stop()
            except Exception as e:
                import traceback
                print(f"Error in stop route: {e}\n{traceback.format_exc()}")
                return jsonify({'error': str(e)}), 500

        @self.flask_app.route('/api/action/stop_immediate', methods=['POST'])
        def stop_immediate():
            try:
                return self._api_stop_immediate()
            except Exception as e:
                import traceback
                print(f"Error in stop_immediate route: {e}\n{traceback.format_exc()}")
                return jsonify({'error': str(e)}), 500

        @self.flask_app.route('/api/action/load', methods=['POST'])
        def load():
            try:
                return self._api_load_videos()
            except Exception as e:
                import traceback
                print(f"Error in load route: {e}\n{traceback.format_exc()}")
                return jsonify({'error': str(e)}), 500

        @self.flask_app.route('/api/action/language', methods=['POST'])
        def language():
            try:
                return self._api_set_language()
            except Exception as e:
                import traceback
                print(f"Error in language route: {e}\n{traceback.format_exc()}")
                return jsonify({'error': str(e)}), 500

        @self.flask_app.route('/static/styles.css')
        def styles():
            try:
                return self._serve_css()
            except Exception as e:
                import traceback
                print(f"Error in styles route: {e}\n{traceback.format_exc()}")
                return f"/* Error: {e} */", 500

        self.http_thread = threading.Thread(target=self._run_flask, args=(port,), daemon=True)
        self.http_thread.start()
        self.http_server_running = True
        
        # Detect local IP for better user guidance
        try:
            import socket
            hostname = socket.gethostname()
            local_ip = socket.gethostbyname(hostname)
            print(f"\n{'='*60}")
            print('[HTTP] HTTP SERVER STARTED')
            print(f"{'='*60}")
            print(f"Local access: http://127.0.0.1:{port}")
            print(f"Network access: http://{local_ip}:{port}")
            print(f"{'='*60}\n")
        except Exception:
            print('[HTTP] HTTP SERVER STARTED')

    def _run_flask(self, port):
        """Run Flask server (called in separate thread)."""
        try:
            # Disable Flask's default logging for cleaner output
            import logging
            log = logging.getLogger('werkzeug')
            log.setLevel(logging.ERROR)

            # Store the werkzeug server reference for shutdown
            from werkzeug.serving import make_server
            self.werkzeug_server = make_server('0.0.0.0', port, self.flask_app, threaded=True)

            # Show success message in GUI
            self.root.after(100, lambda: messagebox.showinfo(
                t('http_server_title'),
                t('http_server_started_message').format(port=port)
            ))

            self.werkzeug_server.serve_forever()
        except Exception as e:
            print(f"Failed to start Flask HTTP server: {e}")
            self.http_server_running = False
            error_text = str(e)
            # Show error message in GUI
            self.root.after(0, lambda: messagebox.showerror(
                t('http_server_error_title'),
                t('http_server_start_error_message').format(error=error_text, port=port)
            ))
    
    def stop_http_server(self):
        """Stop the HTTP server."""
        if hasattr(self, 'werkzeug_server') and self.werkzeug_server:
            try:
                self.werkzeug_server.shutdown()
                self.werkzeug_server = None
                print("HTTP Server stopped")
            except Exception as e:
                print(f"Error stopping HTTP server: {e}")
        elif hasattr(self, 'std_server') and self.std_server:
            try:
                self.std_server.shutdown()
                self.std_server = None
                print("HTTP Server stopped")
            except Exception as e:
                print(f"Error stopping HTTP server: {e}")
        self.http_server_running = False
    
    def _on_http_toggle(self):
        """Handle HTTP server checkbox toggle."""
        if self.http_enabled_var.get():
            # Start server
            port = self.http_port_var.get()
            self.start_http_server(port=port)
        else:
            # Stop server
            self.stop_http_server()
        
        # Save settings
        if hasattr(self, '_save_settings_debounced'):
            self._save_settings_debounced()
    
    def _on_http_port_change(self, event=None):
        """Handle HTTP port change - restart server if running."""
        if self.http_server_running:
            # Restart with new port
            port = self.http_port_var.get()
            self.stop_http_server()
            self.start_http_server(port=port)
        
        # Save settings
        if hasattr(self, '_save_settings_debounced'):
            self._save_settings_debounced()

    def _start_std_server(self, port):
        """Start standard library HTTP server as fallback."""
        gui_instance = self

        class RequestHandler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                pass  # Suppress logging

            def do_GET(self):
                parsed = urlparse(self.path)
                path = parsed.path
                query = parse_qs(parsed.query)
                
                if path == '/':
                    self._send_html(gui_instance._http_index())
                elif path == '/api/status':
                    self._send_json(gui_instance._get_status_data())
                elif path == '/api/videos':
                    # Handle pagination params
                    page = int(query.get('page', [0])[0])
                    page_size = int(query.get('page_size', [1000])[0])
                    filter_status = query.get('filter', [None])[0]
                    self._send_json(gui_instance._get_videos_data(page=page, page_size=page_size, filter_status=filter_status))
                elif path == '/api/settings':
                    self._send_json(gui_instance._get_settings_data())
                elif path == '/static/styles.css':
                    self._send_css(gui_instance._get_css())
                else:
                    self.send_response(404)
                    self.end_headers()

            def do_POST(self):
                parsed = urlparse(self.path)
                path = parsed.path
                content_length = int(self.headers.get('Content-Length', 0))
                body = self.rfile.read(content_length).decode('utf-8') if content_length > 0 else '{}'
                
                try:
                    data = json.loads(body) if body else {}
                except json.JSONDecodeError:
                    data = {}

                if path == '/api/action/start':
                    result = gui_instance._handle_start()
                    self._send_json(result)
                elif path == '/api/action/stop':
                    result = gui_instance._handle_stop()
                    self._send_json(result)
                elif path == '/api/action/stop_immediate':
                    result = gui_instance._handle_stop_immediate()
                    self._send_json(result)
                elif path == '/api/action/load':
                    result = gui_instance._handle_load_videos(data)
                    self._send_json(result)
                elif path == '/api/settings':
                    result = gui_instance._handle_set_settings(data)
                    self._send_json(result)
                else:
                    self.send_response(404)
                    self.end_headers()

            def _send_html(self, content):
                self.send_response(200)
                self.send_header('Content-type', 'text/html; charset=utf-8')
                self.end_headers()
                self.wfile.write(content.encode('utf-8'))

            def _send_json(self, data):
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(data).encode('utf-8'))

            def _send_css(self, content):
                self.send_response(200)
                self.send_header('Content-type', 'text/css; charset=utf-8')
                self.end_headers()
                self.wfile.write(content.encode('utf-8'))

        def run_server():
            try:
                HTTPServer.allow_reuse_address = True
                server = HTTPServer(('0.0.0.0', port), RequestHandler)

                # Detect local IP for better user guidance
                try:
                    import socket
                    hostname = socket.gethostname()
                    local_ip = socket.gethostbyname(hostname)
                    print(f"\n{'='*60}")
                    print('[HTTP] FALLBACK HTTP SERVER STARTED')
                    print(f"{'='*60}")
                    print(f"Local access: http://127.0.0.1:{port}")
                    print(f"Network access: http://{local_ip}:{port}")
                    print(f"{'='*60}\n")

                    # Show success message in GUI
                    gui_instance.root.after(100, lambda: messagebox.showinfo(
                        t('http_server_fallback_title'),
                        t('http_server_started_fallback_message').format(port=port, local_ip=local_ip)
                    ))
                except Exception:
                    print('[HTTP] FALLBACK HTTP SERVER STARTED')

                    # Show simple success message
                    gui_instance.root.after(100, lambda: messagebox.showinfo(
                        t('http_server_title'),
                        t('http_server_started_simple_message').format(port=port)
                    ))

                server.serve_forever()
            except Exception as e:
                print(f"Failed to start HTTP server: {e}")
                gui_instance.http_server_running = False
                error_text = str(e)
                # Show error message in GUI
                gui_instance.root.after(0, lambda: messagebox.showerror(
                    t('http_server_error_title'),
                    t('http_server_start_error_message').format(error=error_text, port=port)
                ))

        self.http_thread = threading.Thread(target=run_server, daemon=True)
        self.http_thread.start()

    # =========================================================================
    # Flask API Endpoints
    # =========================================================================
    
    def _api_start(self):
        """API endpoint to start encoding."""
        result = self._handle_start()
        return jsonify(result)

    def _api_stop(self):
        """API endpoint to stop encoding gracefully."""
        result = self._handle_stop()
        return jsonify(result)

    def _api_stop_immediate(self):
        """API endpoint to stop encoding immediately."""
        result = self._handle_stop_immediate()
        return jsonify(result)

    def _api_load_videos(self):
        """API endpoint to load videos."""
        data = request.get_json() or {}
        result = self._handle_load_videos(data)
        return jsonify(result)

    def _api_set_settings(self):
        """API endpoint to update settings."""
        data = request.get_json() or {}
        result = self._handle_set_settings(data)
        return jsonify(result)
        
    def _api_set_language(self):
        """API endpoint to change language."""
        data = request.get_json() or {}
        result = self._handle_set_language(data)
        return jsonify(result)

    def _serve_css(self):
        """Serve CSS file."""
        return Response(self._get_css(), mimetype='text/css')

    # =========================================================================
    # Action Handlers (thread-safe GUI calls)
    # =========================================================================
    
    def _http_log(self, message):
        """Log HTTP server related messages to console and log file."""
        print(message)
        try:
            from .gui_shared import LOG_WRITER
            if LOG_WRITER and not LOG_WRITER.closed:
                LOG_WRITER.write(message + "\n")
                LOG_WRITER.flush()
        except Exception:
            pass
    
    def _handle_start(self):
        """Handle start encoding request."""
        try:
            self._http_log("[HTTP] Start encoding requested")
            if hasattr(self, 'root'):
                self.root.after(0, self.start_encoding)
            return {'success': True, 'message': t('api_starting')}
        except Exception as e:
            self._http_log(f"[HTTP] Start encoding error: {e}")
            return {'success': False, 'message': str(e)}

    def _handle_stop(self):
        """Handle graceful stop request."""
        try:
            self._http_log("[HTTP] Graceful stop requested")
            if hasattr(self, 'root'):
                self.root.after(0, self.stop_encoding_graceful)
            return {'success': True, 'message': t('api_stopping')}
        except Exception as e:
            self._http_log(f"[HTTP] Graceful stop error: {e}")
            return {'success': False, 'message': str(e)}

    def _handle_stop_immediate(self):
        """Handle immediate stop request."""
        try:
            self._http_log("[HTTP] Immediate stop requested")
            if hasattr(self, 'root'):
                self.root.after(0, self.stop_encoding_immediate)
            return {'success': True, 'message': t('api_immediate_stop')}
        except Exception as e:
            self._http_log(f"[HTTP] Immediate stop error: {e}")
            return {'success': False, 'message': str(e)}

    def _handle_load_videos(self, data):
        """Handle load videos request."""
        try:
            source = data.get('source_path', '')
            dest = data.get('dest_path', '')
            self._http_log(f"[HTTP] Load videos requested: source={source}, dest={dest}")
            
            def do_load():
                if source:
                    self.source_entry.delete(0, 'end')
                    self.source_entry.insert(0, source)
                if dest:
                    self.dest_entry.delete(0, 'end')
                    self.dest_entry.insert(0, dest)
                self.load_videos()
            
            if hasattr(self, 'root'):
                self.root.after(0, do_load)
            return {'success': True, 'message': t('api_loading')}
        except Exception as e:
            self._http_log(f"[HTTP] Load videos error: {e}")
            return {'success': False, 'message': str(e)}
            
    def _handle_set_language(self, data):
        """Handle set language request separately from Desktop GUI."""
        try:
            lang = data.get('language')
            self._http_log(f"[HTTP] Web language change requested: {lang}")
            
            if lang in ('hu', 'en'):
                # ONLY update the HTTP-specific language, do NOT touch Global i18n
                self.http_language = lang
                return {'success': True}
            
            return {'success': False, 'message': 'Invalid language'}
        except Exception as e:
            self._http_log(f"[HTTP] Language set error: {e}")
            return {'success': False, 'message': str(e)}


    def _handle_set_settings(self, data):
        """Handle settings update request."""
        try:
            # Check if encoding is in progress
            is_encoding = getattr(self, 'is_encoding', False)
            encoding_warning = None
            
            if is_encoding and data:
                # Settings are being changed during encoding - add warning
                encoding_warning = t('settings_change_during_encoding_message').replace('\\n', ' ').replace('OK:', '').replace('Mégse:', '').strip()
            
            def do_update():
                # Log the settings change
                self._http_log(f"[HTTP] Settings update: {data}")
                
                # Update VMAF settings with label sync
                if 'min_vmaf' in data:
                    val = float(data['min_vmaf'])
                    self.min_vmaf.set(val)
                    # Update cached value for change detection
                    self._last_min_vmaf_value = val
                    if hasattr(self, 'vmaf_value_label'):
                        self.vmaf_value_label.config(text=f"{val:.1f}")
                
                if 'vmaf_step' in data:
                    val = float(data['vmaf_step'])
                    self.vmaf_step.set(val)
                    self._last_vmaf_step_value = val
                    if hasattr(self, 'vmaf_step_value_label'):
                        self.vmaf_step_value_label.config(text=f"{val:.1f}")
                
                if 'max_encoded_percent' in data:
                    val = int(data['max_encoded_percent'])
                    self.max_encoded_percent.set(val)
                    self._last_max_encoded_value = val
                    if hasattr(self, 'max_encoded_value_label'):
                        self.max_encoded_value_label.config(text=f"{val}%")
                
                if 'max_encoded_mode' in data:
                    val = data['max_encoded_mode']
                    if val in ('full', 'video'):
                        self.max_encoded_mode.set(val)
                        # Update combobox display if exists
                        if hasattr(self, 'max_encoded_mode_combo') and hasattr(self, '_max_encoded_mode_reverse_map'):
                            display_text = self._max_encoded_mode_reverse_map.get(val, t('max_encoded_mode_full'))
                            self.max_encoded_mode_combo.set(display_text)
                
                # Resize settings
                if 'resize_enabled' in data:
                    val = bool(data['resize_enabled'])
                    self.resize_enabled.set(val)
                    self._last_resize_enabled_value = val
                    if hasattr(self, 'toggle_resize_slider'):
                        # Call internal slider toggle without confirmation dialog
                        if val:
                            self.resize_slider.pack(side='left', padx=5)
                            self.resize_value_label.pack(side='left', padx=5)
                        else:
                            self.resize_slider.pack_forget()
                            self.resize_value_label.pack_forget()
                
                if 'resize_height' in data:
                    val = int(data['resize_height'])
                    self.resize_height.set(val)
                    self._last_resize_value = val
                    if hasattr(self, 'resize_value_label'):
                        self.resize_value_label.config(text=f"{val}p")
                
                # Audio compression
                if 'audio_compression_enabled' in data:
                    val = bool(data['audio_compression_enabled'])
                    self.audio_compression_enabled.set(val)
                    self._last_audio_compression_enabled_value = val
                
                # NVENC settings
                if 'nvenc_enabled' in data:
                    val = bool(data['nvenc_enabled'])
                    self.nvenc_enabled.set(val)
                    self._last_nvenc_enabled_value = val
                
                if 'nvenc_worker_count' in data:
                    val = int(data['nvenc_worker_count'])
                    self.nvenc_worker_count.set(val)
                    self.current_nvenc_worker_count = val
                    self._last_nvenc_workers_value = val
                    if hasattr(self, 'nvenc_workers_value_label'):
                        self.nvenc_workers_value_label.config(text=str(val))
                    # Refresh NVENC console tabs
                    if hasattr(self, 'refresh_nvenc_console_tabs'):
                        self.refresh_nvenc_console_tabs(val)
                
                # SVT-AV1 preset
                if 'svt_preset' in data:
                    val = int(data['svt_preset'])
                    self.svt_preset.set(val)
                    self._last_svt_preset_value = val
                    if hasattr(self, 'svt_preset_value_label'):
                        self.svt_preset_value_label.config(text=str(val))

                # SVT-AV1 worker count
                if 'svt_worker_count' in data:
                    val = int(data['svt_worker_count'])
                    self.svt_worker_count.set(val)
                    self.current_svt_worker_count = val
                    self._last_svt_worker_count_value = val
                    if hasattr(self, 'svt_workers_value_label'):
                        self.svt_workers_value_label.config(text=str(val))
                
                # CRF Increment
                if 'crf_increment' in data:
                    val = int(data['crf_increment'])
                    self.crf_increment.set(val)
                    self._last_crf_increment_value = val
                    if hasattr(self, 'crf_increment_value_label'):
                        self.crf_increment_value_label.config(text=str(val))
                
                # Auto VMAF/PSNR
                if 'auto_vmaf_psnr' in data:
                    val = bool(data['auto_vmaf_psnr'])
                    self.auto_vmaf_psnr.set(val)
                    self._last_auto_vmaf_psnr_value = val
                
                # Skip AV1 files
                if 'skip_av1_files' in data:
                    val = bool(data['skip_av1_files'])
                    self.skip_av1_files.set(val)
                    self._last_skip_av1_value = val
                
                # Trigger settings save
                if hasattr(self, '_save_settings_debounced'):
                    self._save_settings_debounced()
            
            if hasattr(self, 'root'):
                self.root.after(0, do_update)
            
            # Build response
            response = {'success': True, 'message': t('api_settings_updated')}
            if encoding_warning:
                response['encoding_warning'] = encoding_warning
            
            return response
        except Exception as e:
            self._http_log(f"[HTTP] Settings update error: {e}")
            return {'success': False, 'message': str(e)}

    def _api_get_videos(self):
        """API endpoint for videos with pagination."""
        try:
            from flask import request
            page = int(request.args.get('page', 0))
            page_size = int(request.args.get('page_size', 100))
            filter_status = request.args.get('filter', None)
            return jsonify(self._get_videos_data(page=page, page_size=page_size, filter_status=filter_status))
        except Exception as e:
            self._http_log(f"[HTTP] Get videos error: {e}")
            return jsonify(self._get_videos_data())

    # =========================================================================
    # Data Retrieval Methods
    # =========================================================================
    
    def _get_status_data(self):
        """Get current application status."""
        is_encoding = getattr(self, 'is_encoding', False)
        is_loading = getattr(self, 'is_loading_videos', False)
        graceful_stop = getattr(self, 'graceful_stop_requested', False)
        
        # Count videos by status
        total = 0
        completed = 0
        pending = 0
        encoding = 0
        failed = 0
        
        # CRITICAL FIX: Check if app is closing BEFORE accessing Tkinter widgets!
        # Tkinter is NOT thread-safe - calling root.call() after root.destroy()
        # causes DEADLOCK, not exception!
        from .gui_shared import is_app_closing
        if is_app_closing():
            # Return cached/approximate values when app is closing
            total = len(getattr(self, 'video_items', {}))
        elif hasattr(self, 'video_items') and hasattr(self, 'tree'):
            try:
                for video_path, item_id in list(self.video_items.items()):
                    try:
                        # Access tree data - NOT thread-safe but acceptable for HTTP status
                        values = self.root.call(self.tree._w, 'item', item_id, '-values')
                        tags = self.root.call(self.tree._w, 'item', item_id, '-tags')
                        total += 1

                        if 'completed' in tags or 'completed_copy' in tags or (values and '[OK]' in str(values[1])):
                            completed += 1
                        elif 'encoding' in tags:
                            encoding += 1
                        elif 'failed' in tags:
                            failed += 1
                        else:
                            pending += 1
                    except Exception:
                        pass
            except Exception:
                pass
        
        # Get loading progress details
        loading_progress = None
        if is_loading and hasattr(self, 'loading_progress'):
            lp = self.loading_progress
            loading_progress = {
                'phase': lp.get('phase', 'unknown'),
                'phase_text': lp.get('phase_text', ''),
                'total_files': lp.get('total_files', 0),
                'processed_files': lp.get('processed_files', 0),
                'percent': lp.get('percent', 0),
                'estimated_remaining_seconds': lp.get('estimated_remaining_seconds'),
                'elapsed_seconds': time.time() - lp.get('start_time', time.time()) if lp.get('start_time') else 0
            }
        
        return {
            'is_encoding': is_encoding,
            'is_loading': is_loading,
            'graceful_stop_requested': graceful_stop,
            'total_videos': total,
            'completed': completed,
            'pending': pending,
            'encoding': encoding,
            'failed': failed,
            'source_path': str(self.source_path) if hasattr(self, 'source_path') and self.source_path else '',
            'dest_path': str(self.dest_path) if hasattr(self, 'dest_path') and self.dest_path else '',
            'loading_progress': loading_progress
        }

    def _get_videos_data(self, page=0, page_size=1000, filter_status=None):
        """Get videos with pagination.
        
        Args:
            page: Page number (0-indexed)
            page_size: Number of videos per page (default 1000)
            filter_status: Optional status filter ('pending', 'encoding', 'completed', 'failed')
            
        Returns:
            dict with 'videos' list, 'total', 'page', 'page_size', 'total_pages', 'summary'
        """
        result = {
            'videos': [],
            'total': 0,
            'page': page,
            'page_size': page_size,
            'total_pages': 0,
            'summary': {
                'pending': 0,
                'encoding': 0,
                'completed': 0,
                'failed': 0,
                # Size summary for completed videos
                'total_orig_size': 0,
                'total_new_size': 0,
                'total_orig_size_formatted': '-',
                'total_new_size_formatted': '-',
                'savings_percent': 0,
                'savings_percent_formatted': '-',
                'encoded_count': 0
            }
        }
        
        if not hasattr(self, 'video_items') or not hasattr(self, 'tree'):
            return result

        # CRITICAL FIX: Check if app is closing BEFORE accessing Tkinter widgets!
        # Tkinter is NOT thread-safe - calling root.call() after root.destroy()
        # causes DEADLOCK, not exception!
        from .gui_shared import is_app_closing
        if is_app_closing():
            # Return empty result when app is closing to avoid deadlock
            result['total'] = len(self.video_items)
            return result

        try:
            # Get column index mapping
            col_idx = getattr(self, 'COLUMN_INDEX', {
                'denoise': 0, 'video_name': 1, 'status': 2, 'cq': 3, 'vmaf': 4, 'psnr': 5,
                'progress': 6, 'orig_size': 7, 'new_size': 8, 'size_change': 9,
                'duration': 10, 'frames': 11, 'completed_date': 12
            })

            # Collect all videos with status for filtering and summary
            all_videos = []
            total_orig_bytes = 0
            total_new_bytes = 0
            encoded_count = 0

            for video_path, item_id in list(self.video_items.items()):
                try:
                    # Access tree data - NOT thread-safe but acceptable for HTTP API
                    values = self.root.call(self.tree._w, 'item', item_id, '-values')
                    tags = self.root.call(self.tree._w, 'item', item_id, '-tags')
                    order = self.video_order.get(video_path, 0) if hasattr(self, 'video_order') else 0
                    
                    # Determine status type for styling
                    status_type = 'pending'
                    if 'completed' in tags or 'completed_copy' in tags:
                        status_type = 'completed'
                    elif 'encoding' in tags or 'encoding_nvenc' in tags or 'encoding_svt' in tags:
                        status_type = 'encoding'
                    elif 'failed' in tags:
                        status_type = 'failed'
                    
                    # Update summary
                    result['summary'][status_type] = result['summary'].get(status_type, 0) + 1
                    
                    # Get size values
                    orig_size_str = values[col_idx.get('orig_size', 6)] if len(values) > col_idx.get('orig_size', 6) else '-'
                    new_size_str = values[col_idx.get('new_size', 7)] if len(values) > col_idx.get('new_size', 7) else '-'
                    
                    # Calculate size totals for completed videos
                    if status_type == 'completed':
                        try:
                            orig_bytes = parse_size_to_bytes(orig_size_str)
                            new_bytes = parse_size_to_bytes(new_size_str)
                            
                            if orig_bytes is not None:
                                total_orig_bytes += orig_bytes
                            if new_bytes is not None:
                                total_new_bytes += new_bytes
                            
                            if orig_bytes is not None or new_bytes is not None:
                                encoded_count += 1
                        except (ValueError, TypeError):
                            pass
                    
                    # Filter if needed
                    if filter_status and status_type != filter_status:
                        continue
                    
                    video_data = {
                        'order': order,
                        'path': str(video_path),
                        'denoise': values[col_idx.get('denoise', 0)] if len(values) > col_idx.get('denoise', 0) else '',
                        'name': values[col_idx.get('video_name', 1)] if len(values) > col_idx.get('video_name', 1) else Path(video_path).name,
                        'status': values[col_idx.get('status', 2)] if len(values) > col_idx.get('status', 2) else '-',
                        'status_type': status_type,
                        'cq': values[col_idx.get('cq', 3)] if len(values) > col_idx.get('cq', 3) else '-',
                        'vmaf': values[col_idx.get('vmaf', 4)] if len(values) > col_idx.get('vmaf', 4) else '-',
                        'psnr': values[col_idx.get('psnr', 5)] if len(values) > col_idx.get('psnr', 5) else '-',
                        'progress': values[col_idx.get('progress', 6)] if len(values) > col_idx.get('progress', 6) else '-',
                        'orig_size': orig_size_str,
                        'new_size': new_size_str,
                        'size_change': values[col_idx.get('size_change', 9)] if len(values) > col_idx.get('size_change', 9) else '-',
                        'eta': values[col_idx.get('completed_date', 12)] if len(values) > col_idx.get('completed_date', 12) else '-'
                    }
                    all_videos.append(video_data)
                except Exception:
                    pass
            
            # Calculate size summary for completed videos
            if encoded_count > 0 and total_orig_bytes > 0:
                savings_percent = ((total_new_bytes - total_orig_bytes) / total_orig_bytes) * 100
                result['summary']['total_orig_size'] = total_orig_bytes
                result['summary']['total_new_size'] = total_new_bytes
                result['summary']['total_orig_size_formatted'] = format_size_auto(total_orig_bytes)
                result['summary']['total_new_size_formatted'] = format_size_auto(total_new_bytes)
                result['summary']['savings_percent'] = savings_percent
                result['summary']['savings_percent_formatted'] = f"{savings_percent:+.2f}%"
                result['summary']['encoded_count'] = encoded_count
            
            # Sort by order
            all_videos.sort(key=lambda x: x.get('order', 0))
            
            # Apply pagination
            result['total'] = len(all_videos)
            result['total_pages'] = (len(all_videos) + page_size - 1) // page_size if page_size > 0 else 1
            
            start_idx = page * page_size
            end_idx = start_idx + page_size
            result['videos'] = all_videos[start_idx:end_idx]
            
        except Exception:
            pass
        
        return result

    def _get_settings_data(self):
        """Get current settings."""
        # Helper to safely get tk var value
        def get_tk_val(var_name, default):
            if not hasattr(self, var_name): return default
            var = getattr(self, var_name)
            try:
                # Direct get might fail in thread, use root.call if possible 
                # but Var objects are tricky. DoubleVar.get() usually is thread-safe on read,
                # but let's be robust.
                return var.get()
            except Exception:
                return default

        return {
            'min_vmaf': float(get_tk_val('min_vmaf', 97.5)),
            'vmaf_step': float(get_tk_val('vmaf_step', 0.25)),
            'max_encoded_percent': float(get_tk_val('max_encoded_percent', 75.0)),
            'max_encoded_mode': str(get_tk_val('max_encoded_mode', 'full')),
            'resize_enabled': bool(get_tk_val('resize_enabled', False)),
            'resize_height': int(get_tk_val('resize_height', 1080)),
            'audio_compression_enabled': bool(get_tk_val('audio_compression_enabled', False)),
            'audio_compression_method': str(get_tk_val('audio_compression_method', 'fast')),
            'nvenc_enabled': bool(get_tk_val('nvenc_enabled', True)),
            'svt_preset': int(get_tk_val('svt_preset', 2)),
            'svt_worker_count': int(get_tk_val('svt_worker_count', 1)),
            'nvenc_worker_count': int(get_tk_val('nvenc_worker_count', 1)),
            'crf_increment': int(get_tk_val('crf_increment', 1)),
            'debug_mode': bool(get_tk_val('debug_mode', False)),
            'auto_vmaf_psnr': bool(get_tk_val('auto_vmaf_psnr', False)),
            # Use self.source_path/dest_path instead of entry.get() to avoid TclError in non-main thread
            'source_path': str(self.source_path) if hasattr(self, 'source_path') and self.source_path else '',
            'dest_path': str(self.dest_path) if hasattr(self, 'dest_path') and self.dest_path else '',
            'language': getattr(self, 'http_language', 'hu')
        }

    # =========================================================================
    # HTML/CSS Content
    # =========================================================================
    
    def _http_index(self):
        """Generate the main HTML page."""
        html = self._get_html()
        # Ensure proper UTF-8 encoding, replacing invalid surrogates
        if isinstance(html, str):
            html = html.encode('utf-8', errors='replace').decode('utf-8')
        return html

    def _get_html(self):
        """Return the full HTML content for the web interface."""
        return '''<!DOCTYPE html>
<html lang="''' + getattr(self, 'http_language', 'hu') + '''">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>AV1 Batch Encoder</title>
    <link rel="stylesheet" href="/static/styles.css">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
</head>
<body>
    <div id="connection-lost-banner" class="connection-lost-banner" style="display:none;">
        <span>[WARN] Kapcsolat megszakadt! (Connection Lost)</span>
    </div>
    <div class="app-container">
        <!-- Header -->
        <header class="header">
            <div class="header-left">
                <h1 class="logo">&#x1F3AC; ''' + self._t('app_title') + '''</h1>
            </div>
            <div class="header-right">
                <div id="status-badge" class="status-badge idle">
                    <span class="status-dot"></span>
                    <span id="status-text">''' + self._t('http_idle') + '''</span>
                </div>
                <select id="lang-selector" class="lang-selector">
                    <option value="hu">HUN</option>
                    <option value="en">ENG</option>
                </select>
            </div>
        </header>

        <!-- Loading Progress Panel -->
        <div id="loading-details" class="loading-details" style="display:none;">
        </div>

        <div class="main-content">
            <!-- Left Panel: Settings -->
            <aside class="settings-panel">
                <div class="panel-section">
                    <h3>''' + '&#x1F4C1; ' + self._t('http_folders') + '''</h3>
                    <div class="form-group">
                        <label>''' + self._t('source') + '''</label>
                        <input type="text" id="source-path" class="form-input" placeholder="C:\\Videos\\Source">
                    </div>
                    <div class="form-group">
                        <label>''' + self._t('dest') + '''</label>
                        <input type="text" id="dest-path" class="form-input" placeholder="C:\\Videos\\Encoded">
                    </div>
                    <button id="btn-load" class="btn btn-secondary btn-full">
                        <span>&#x1F4C2;</span> ''' + self._t('load_videos') + '''
                    </button>
                </div>

                <div class="panel-section">
                    <h3>''' + '&#x2699;&#xFE0F; ' + self._t('http_encoding_settings') + '''</h3>
                    
                    <div class="form-group">
                        <label>''' + self._t('min_vmaf') + '''</label>
                        <div class="slider-container">
                            <input type="range" id="min-vmaf" min="85" max="99.9" step="0.1" value="97.5">
                            <span id="min-vmaf-value" class="slider-value">97.50</span>
                        </div>
                    </div>

                    <div class="form-group">
                        <label>''' + self._t('vmaf_fallback') + '''</label>
                        <div class="slider-container">
                            <input type="range" id="vmaf-step" min="0.1" max="5" step="0.05" value="0.25">
                            <span id="vmaf-step-value" class="slider-value">0.25</span>
                        </div>
                    </div>

                    <div class="form-group">
                        <label>''' + self._t('max_encoded') + '''</label>
                        <div class="mode-select-container">
                            <select id="max-encoded-mode" class="mode-select">
                                <option value="full">''' + self._t('max_encoded_mode_full') + '''</option>
                                <option value="video">''' + self._t('max_encoded_mode_video') + '''</option>
                            </select>
                        </div>
                        <div class="slider-container">
                            <input type="range" id="max-encoded" min="1" max="100" step="1" value="75">
                            <span id="max-encoded-value" class="slider-value">75%</span>
                        </div>
                    </div>

                    <div class="form-group">
                        <label>SVT-AV1 Preset</label>
                        <div class="slider-container">
                            <input type="range" id="svt-preset" min="1" max="12" step="1" value="2">
                            <span id="svt-preset-value" class="slider-value">2</span>
                        </div>
                    </div>

                    <div class="form-group">
                        <label>''' + self._t('svt_workers') + '''</label>
                        <div class="slider-container">
                            <input type="range" id="svt-workers" min="1" max="8" step="1" value="1">
                            <span id="svt-workers-value" class="slider-value">1</span>
                        </div>
                    </div>

                    <div class="form-group">
                        <label>''' + self._t('crf_increment') + '''</label>
                        <div class="slider-container">
                            <input type="range" id="crf-increment" min="1" max="4" step="1" value="1">
                            <span id="crf-increment-value" class="slider-value">1</span>
                        </div>
                    </div>

                    <div class="checkbox-group">
                        <label class="checkbox-label">
                            <input type="checkbox" id="nvenc-enabled" checked>
                            <span>''' + '&#x1F5A5;&#xFE0F; ' + self._t('context_nvenc_encode') + '''</span>
                        </label>
                    </div>

                    <div class="form-group">
                        <label>''' + self._t('nvenc_workers') + '''</label>
                        <div class="slider-container">
                            <input type="range" id="nvenc-workers" min="1" max="3" step="1" value="1">
                            <span id="nvenc-workers-value" class="slider-value">1</span>
                        </div>
                    </div>

                    <div class="checkbox-group">
                        <label class="checkbox-label">
                            <input type="checkbox" id="resize-enabled">
                            <span>''' + '&#x2195;&#xFE0F; ' + self._t('http_resize') + '''</span>
                        </label>
                    </div>

                    <div class="form-group" id="resize-height-group" style="display:none;">
                        <label>''' + self._t('resize_height') + '''</label>
                        <div class="slider-container">
                            <input type="range" id="resize-height" min="360" max="2160" step="10" value="1080">
                            <span id="resize-height-value" class="slider-value">1080p</span>
                        </div>
                    </div>

                    <div class="checkbox-group">
                        <label class="checkbox-label">
                            <input type="checkbox" id="audio-compression">
                            <span>''' + '&#x1F50A; ' + self._t('audio_compression') + '''</span>
                        </label>
                    </div>

                    <button id="btn-save-settings" class="btn btn-outline btn-full">
                        ''' + '&#x1F4BE; ' + self._t('http_save_settings') + '''
                    </button>
                </div>
            </aside>

            <!-- Main Area: Video List -->
            <main class="video-panel">
                <div class="video-header">
                    <div class="video-stats">
                        <div class="stat-item">
                            <span class="stat-value" id="stat-total">0</span>
                            <span class="stat-label">''' + self._t('http_total') + '''</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-value stat-completed" id="stat-completed">0</span>
                            <span class="stat-label">''' + self._t('column_completed') + '''</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-value stat-encoding" id="stat-encoding">0</span>
                            <span class="stat-label">''' + self._t('http_in_progress') + '''</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-value stat-pending" id="stat-pending">0</span>
                            <span class="stat-label">''' + self._t('http_pending') + '''</span>
                        </div>
                    </div>
                    <div class="video-actions">
                        <button id="btn-start" class="btn btn-primary">
                            <span>&#x25B6;&#xFE0F;</span> ''' + self._t('btn_start') + '''
                        </button>
                        <button id="btn-stop" class="btn btn-warning" disabled>
                            <span>&#x23F8;&#xFE0F;</span> ''' + self._t('btn_stop') + '''
                        </button>
                        <button id="btn-stop-immediate" class="btn btn-danger" disabled>
                            <span>&#x23F9;&#xFE0F;</span> ''' + self._t('btn_immediate_stop') + '''
                        </button>
                    </div>
                    <!-- Mobile size summary (only visible on mobile) -->
                    <div id="mobile-size-summary" class="mobile-size-summary mobile-only">
                        <span class="size-item size-orig" id="mobile-orig-size">''' + self._t('column_orig_size') + ''': -</span>
                        <span class="size-item size-new" id="mobile-new-size">''' + self._t('column_new_size') + ''': -</span>
                        <span class="size-item size-savings" id="mobile-savings">''' + self._t('column_size_change') + ''': -</span>
                    </div>
                </div>

                <div class="video-table-container">
                    <div id="video-summary" class="video-summary desktop-only">
                        <span class="summary-total">''' + self._t('http_total') + ''': 0</span>
                        <span class="summary-pending">''' + self._t('http_pending') + ''': 0</span>
                        <span class="summary-encoding">''' + self._t('http_encoding') + ''': 0</span>
                        <span class="summary-completed">''' + self._t('column_completed') + ''': 0</span>
                        <span class="summary-failed">''' + self._t('status_failed') + ''': 0</span>
                    </div>
                    
                    <!-- Desktop: Table View -->
                    <table class="video-table desktop-only" style="display: table;">
                        <thead>
                            <tr>
                                <th class="col-order">#<div class="resizer"></div></th>
                                <th class="col-denoise">''' + self._t('column_denoise') + '''<div class="resizer"></div></th>
                                <th class="col-name">''' + self._t('column_video') + '''<div class="resizer"></div></th>
                                <th class="col-status">''' + self._t('column_status') + '''<div class="resizer"></div></th>
                                <th class="col-cq">''' + self._t('column_cq') + '''<div class="resizer"></div></th>
                                <th class="col-vmaf">''' + self._t('column_vmaf') + '''<div class="resizer"></div></th>
                                <th class="col-progress">''' + self._t('column_progress') + '''<div class="resizer"></div></th>
                                <th class="col-orig">''' + self._t('column_orig_size') + '''<div class="resizer"></div></th>
                                <th class="col-new">''' + self._t('column_new_size') + '''<div class="resizer"></div></th>
                                <th class="col-change">''' + self._t('column_size_change') + '''<div class="resizer"></div></th>
                                <th class="col-eta">''' + self._t('http_eta') + '''<div class="resizer"></div></th>
                            </tr>
                        </thead>
                        <tbody id="video-list">
                            <tr class="empty-row">
                                <td colspan="11">''' + self._t('msg_no_videos') + '''</td>
                            </tr>
                        </tbody>
                    </table>
                    
                    <!-- Mobile: Card View -->
                    <div id="video-cards" class="video-cards mobile-only">
                        <div class="empty-card">
                            <span>&#x1F4C2;</span>
                            <p>''' + self._t('msg_no_videos') + '''</p>
                            <p class="hint">''' + self._t('http_load_hint') + '''</p>
                        </div>
                    </div>
                </div>
            </main>
        </div>

        <footer class="footer">
            <span>AV1 Batch Encoder Web Interface</span>
            <span id="refresh-indicator">&#x1F504; ''' + self._t('http_auto_refresh') + '''</span>
        </footer>
    </div>

    <script>
        // Localization constants
        const MSG_LOADING_STATUS = \'''' + self._t('http_loading_status') + '''\';
        const MSG_ELAPSED = \'''' + self._t('http_elapsed') + '''\';
        const MSG_MOBILE_ORIG = \'''' + self._t('mobile_orig_label') + '''\';
        const MSG_MOBILE_NEW = \'''' + self._t('mobile_new_label') + '''\';
        const MSG_MOBILE_SAVINGS = \'''' + self._t('mobile_savings_label') + '''\';
        
        // Summary row localization
        const MSG_SUM_TOTAL = \'''' + self._t('http_sum_total') + '''\';
        const MSG_SUM_PENDING = \'''' + self._t('http_sum_pending') + '''\';
        const MSG_SUM_ENCODING = \'''' + self._t('http_sum_encoding') + '''\';
        const MSG_SUM_COMPLETED = \'''' + self._t('http_sum_completed') + '''\';
        const MSG_SUM_FAILED = \'''' + self._t('http_sum_failed') + '''\';
        const MSG_SUM_ORIG = \'''' + self._t('http_sum_orig') + '''\';
        const MSG_SUM_NEW = \'''' + self._t('http_sum_new') + '''\';
        const MSG_SUM_SAVINGS = \'''' + self._t('http_sum_savings') + '''\';

        // Centralized decorative parser for web UI only.
        const WEB_GLYPHS = Object.freeze({
            ok: '\u2713',
            error: '\u274C',
            warn: '\u26A0\uFE0F',
            info: '\u2139\uFE0F',
            pending: '\u23F3',
            encoding: '\uD83D\uDD04',
            folder: '\uD83D\uDCC2',
            dir: '\uD83D\uDCC1',
            sparkle: '\u2728',
            stop: '\u23F9\uFE0F',
            play: '\u25B6\uFE0F',
            copy: '\uD83D\uDCCB',
            probe: '\uD83D\uDD0E',
            search: '\uD83D\uDD0D',
            stats: '\uD83D\uDCCA',
            robot: '\uD83E\uDD16',
            refresh: '\uD83D\uDD04'
        });

        const WEB_TEXT_TOKEN_MAP = Object.freeze([
            ['[OK]', WEB_GLYPHS.ok],
            ['[ERROR]', WEB_GLYPHS.error],
            ['[WARN]', WEB_GLYPHS.warn],
            ['[INFO]', WEB_GLYPHS.info],
            ['[DIR]', WEB_GLYPHS.dir],
            ['[COPY]', WEB_GLYPHS.copy],
            ['[PROBE]', WEB_GLYPHS.probe],
            ['[SEARCH]', WEB_GLYPHS.search],
            ['[STATS]', WEB_GLYPHS.stats],
            ['[STOP]', WEB_GLYPHS.stop],
            ['[PLAY]', WEB_GLYPHS.play],
            ['[AUTOTEST]', WEB_GLYPHS.robot],
            ['[REFRESH]', WEB_GLYPHS.refresh],
        ]);

        const WEB_MOJIBAKE_MAP = Object.freeze([]);

        function toWebDecorativeText(value) {
            if (value === null || value === undefined) return '';
            let text = String(value);
            WEB_MOJIBAKE_MAP.forEach(([broken, fixed]) => {
                text = text.split(broken).join(fixed);
            });
            WEB_TEXT_TOKEN_MAP.forEach(([token, glyph]) => {
                text = text.split(token).join(glyph);
            });
            return text;
        }

        // =====================================================================
        // State Management
        // =====================================================================
        let currentSettings = {};
        let isEncoding = false;
        
        // =====================================================================
        // UI Update Functions
        // =====================================================================
        
        function updateStatus(data) {
            const badge = document.getElementById('status-badge');
            const text = document.getElementById('status-text');
            const loadingDetails = document.getElementById('loading-details');
            
            if (data.is_loading) {
                badge.className = 'status-badge loading';
                
                // Show detailed loading progress
                if (data.loading_progress) {
                    const lp = data.loading_progress;
                    let statusText = lp.phase_text || MSG_LOADING_STATUS;
                    text.textContent = statusText;
                    
                    // Show loading details panel
                    if (loadingDetails) {
                        let detailsHtml = `<div class="loading-progress-bar"><div class="loading-progress-fill" style="width: ${lp.percent || 0}%"></div></div>`;
                        detailsHtml += `<div class="loading-progress-text">${lp.phase_text || ''}</div>`;
                        
                        if (lp.elapsed_seconds > 0) {
                            const elapsed = Math.floor(lp.elapsed_seconds);
                            const elapsedMin = Math.floor(elapsed / 60);
                            const elapsedSec = elapsed % 60;
                            detailsHtml += `<div class="loading-elapsed">${MSG_ELAPSED} ${elapsedMin}:${String(elapsedSec).padStart(2, '0')}</div>`;
                        }
                        
                        loadingDetails.innerHTML = detailsHtml;
                        loadingDetails.style.display = 'block';
                    }

                } else {
                    text.textContent = \'''' + self._t('http_loading') + '''\';
                    if (loadingDetails) loadingDetails.style.display = 'none';
                }
            } else if (data.is_encoding) {
                if (loadingDetails) loadingDetails.style.display = 'none';
                if (data.graceful_stop_requested) {
                    badge.className = 'status-badge stopping';
                    text.textContent = \'''' + self._t('http_stopping') + '''\';
                } else {
                    badge.className = 'status-badge running';
                    text.textContent = \'''' + self._t('http_encoding') + '''\';
                }
            } else {
                badge.className = 'status-badge idle';
                text.textContent = \'''' + self._t('http_idle') + '''\';
                if (loadingDetails) loadingDetails.style.display = 'none';
            }
            
            isEncoding = data.is_encoding;
            
            // Update stats
            document.getElementById('stat-total').textContent = data.total_videos || 0;
            document.getElementById('stat-completed').textContent = data.completed || 0;
            document.getElementById('stat-encoding').textContent = data.encoding || 0;
            document.getElementById('stat-pending').textContent = data.pending || 0;
            
            // Update buttons
            document.getElementById('btn-start').disabled = data.is_encoding || data.is_loading;
            document.getElementById('btn-stop').disabled = !data.is_encoding;
            document.getElementById('btn-stop-immediate').disabled = !data.is_encoding;
            document.getElementById('btn-load').disabled = data.is_encoding;
            // Note: paths are updated via fetchAndSyncSettings -> updateSettings
        }
        
        function updateVideoList(data) {
            const tbody = document.getElementById('video-list');
            const cardsContainer = document.getElementById('video-cards');
            
            // Handle new pagination response format
            const videos = Array.isArray(data) ? data : (data.videos || []);
            const summary = data.summary || {};
            const total = data.total || videos.length;
            const page = data.page || 0;
            const totalPages = data.total_pages || 1;
            
            // Update summary display if exists
            const summaryEl = document.getElementById('video-summary');
            if (summaryEl) {
                // Build size summary part only if there are completed videos with sizes
                let sizeSummaryHtml = '';
                if (summary.encoded_count > 0 && summary.total_orig_size_formatted && summary.total_orig_size_formatted !== '-') {
                    const savingsClass = (summary.savings_percent || 0) < 0 ? 'size-reduced' : 'size-increased';
                    sizeSummaryHtml = `
                        <span class="summary-divider">|</span>
                        <span class="summary-orig-size">${MSG_SUM_ORIG}: ${summary.total_orig_size_formatted}</span>
                        <span class="summary-new-size">${MSG_SUM_NEW}: ${summary.total_new_size_formatted}</span>
                        <span class="summary-savings ${savingsClass}">${MSG_SUM_SAVINGS}: ${summary.savings_percent_formatted}</span>
                    `;
                }
                
                summaryEl.innerHTML = `
                    <span class="summary-total">${MSG_SUM_TOTAL}: ${total.toLocaleString()}</span>
                    <span class="summary-pending">${MSG_SUM_PENDING}: ${(summary.pending || 0).toLocaleString()}</span>
                    <span class="summary-encoding">${MSG_SUM_ENCODING}: ${(summary.encoding || 0).toLocaleString()}</span>
                    <span class="summary-completed">${MSG_SUM_COMPLETED}: ${(summary.completed || 0).toLocaleString()}</span>
                    <span class="summary-failed">${MSG_SUM_FAILED}: ${(summary.failed || 0).toLocaleString()}</span>
                    ${sizeSummaryHtml}
                `;
            }
            
            // Update mobile size summary
            const mobileOrigSize = document.getElementById('mobile-orig-size');
            const mobileNewSize = document.getElementById('mobile-new-size');
            const mobileSavings = document.getElementById('mobile-savings');
            
            if (mobileOrigSize && mobileNewSize && mobileSavings) {
                if (summary.encoded_count > 0 && summary.total_orig_size_formatted && summary.total_orig_size_formatted !== '-') {
                    mobileOrigSize.textContent = `${MSG_MOBILE_ORIG} ${summary.total_orig_size_formatted}`;
                    mobileNewSize.textContent = `${MSG_MOBILE_NEW} ${summary.total_new_size_formatted}`;
                    mobileSavings.textContent = `${MSG_MOBILE_SAVINGS} ${summary.savings_percent_formatted}`;
                    
                    // Update savings color class
                    mobileSavings.classList.remove('size-reduced', 'size-increased');
                    if ((summary.savings_percent || 0) < 0) {
                        mobileSavings.classList.add('size-reduced');
                    } else {
                        mobileSavings.classList.add('size-increased');
                    }
                } else {
                    mobileOrigSize.textContent = `${MSG_MOBILE_ORIG} -`;
                    mobileNewSize.textContent = `${MSG_MOBILE_NEW} -`;
                    mobileSavings.textContent = `${MSG_MOBILE_SAVINGS} -`;
                    mobileSavings.classList.remove('size-reduced', 'size-increased');
                }
            }
            
            if (!videos || videos.length === 0) {
                if (page === 0) {
                    tbody.innerHTML = '<tr class="empty-row"><td colspan="11">''' + self._t('msg_no_videos') + '''</td></tr>';
                    if (cardsContainer) {
                        cardsContainer.innerHTML = `
                            <div class="empty-card">
                                <span>&#x1F4C2;</span>
                                <p>''' + self._t('msg_no_videos') + '''</p>
                                <p class="hint">''' + self._t('http_load_hint') + '''</p>
                            </div>
                        `;
                    }
                }
                return;
            }
            
            // Generate table rows (desktop)
            const rows = videos.map((v, i) => `
                <tr class="video-row ${v.status_type}">
                    <td class="col-order">${v.order || i+1}</td>
                    <td class="col-denoise">${toWebDecorativeText(v.denoise)}</td>
                    <td class="col-name" title="${v.path}">${v.name}</td>
                    <td class="col-status">${toWebDecorativeText(v.status)}</td>
                    <td class="col-cq">${v.cq}</td>
                    <td class="col-vmaf">${v.vmaf}</td>
                    <td class="col-progress">${v.progress}</td>
                    <td class="col-orig">${v.orig_size}</td>
                    <td class="col-new">${v.new_size}</td>
                    <td class="col-change">${v.size_change}</td>
                    <td class="col-eta">${v.eta || '-'}</td>
                </tr>
            `).join('');
            
            // Generate cards (mobile)
            const cards = videos.map((v, i) => {
                // Determine status icon and color class
                let statusIcon = WEB_GLYPHS.pending;
                let statusClass = 'pending';
                if (v.status_type === 'completed') {
                    statusIcon = WEB_GLYPHS.ok;
                    statusClass = 'completed';
                } else if (v.status_type === 'encoding') {
                    statusIcon = WEB_GLYPHS.encoding;
                    statusClass = 'encoding';
                } else if (v.status_type === 'failed') {
                    statusIcon = WEB_GLYPHS.error;
                    statusClass = 'failed';
                }
                
                // Format size change with color indicator
                let sizeChangeClass = '';
                const sizeChange = v.size_change || '-';
                if (sizeChange.includes('-')) {
                    sizeChangeClass = 'size-reduced';
                } else if (sizeChange.includes('+')) {
                    sizeChangeClass = 'size-increased';
                }
                
                return `
                <div class="video-card ${statusClass}">
                    <div class="card-header">
                        <span class="card-order">#${v.order || i+1}</span>
                        ${(() => { const dn = (v.denoise || '').split(WEB_GLYPHS.ok).length - 1; return dn >= 1 ? `<span class="denoise-badge" title="${dn === 4 ? 'Ultra strong' : dn === 3 ? 'Very strong' : dn === 2 ? 'Strong' : 'Light'} denoise">${WEB_GLYPHS.ok.repeat(dn)}</span>` : ''; })()}
                        <span class="card-name" title="${v.path}">${v.name}</span>
                    </div>
                    <div class="card-status">
                        <span class="status-icon">${statusIcon}</span>
                        <span class="status-text">${toWebDecorativeText(v.status)}</span>
                    </div>
                    <div class="card-metrics">
                        <div class="metric">
                            <span class="metric-label">VMAF</span>
                            <span class="metric-value">${v.vmaf}</span>
                        </div>
                        <div class="metric">
                            <span class="metric-label">CQ</span>
                            <span class="metric-value">${v.cq}</span>
                        </div>
                        <div class="metric">
                            <span class="metric-label">''' + self._t('column_progress') + '''</span>
                            <span class="metric-value">${v.progress}</span>
                        </div>
                    </div>
                    <div class="card-sizes">
                        <span class="size-orig">${v.orig_size}</span>
                        <span class="size-arrow">-></span>
                        <span class="size-new">${v.new_size}</span>
                        <span class="size-change ${sizeChangeClass}">${sizeChange}</span>
                    </div>
                    <div class="card-eta">
                        <span class="eta-label">ETA:</span>
                        <span class="eta-value">${v.eta || '-'}</span>
                    </div>
                </div>
                `;
            }).join('');
            
            // If first page, replace content; otherwise append
            if (page === 0) {
                tbody.innerHTML = rows;
                if (cardsContainer) cardsContainer.innerHTML = cards;
            } else {
                tbody.insertAdjacentHTML('beforeend', rows);
                if (cardsContainer) cardsContainer.insertAdjacentHTML('beforeend', cards);
            }
            
            // Update pagination state
            window.videoState = {
                currentPage: page,
                totalPages: totalPages,
                total: total,
                isLoading: false
            };
        }
        
        function updateSettings(settings) {
            currentSettings = settings;
            
            document.getElementById('min-vmaf').value = settings.min_vmaf || 97.5;
            document.getElementById('min-vmaf-value').textContent = (settings.min_vmaf || 97.5).toFixed(2);
            
            document.getElementById('vmaf-step').value = settings.vmaf_step || 0.25;
            document.getElementById('vmaf-step-value').textContent = (settings.vmaf_step || 0.25).toFixed(2);
            
            document.getElementById('max-encoded').value = settings.max_encoded_percent || 75;
            document.getElementById('max-encoded-value').textContent = (settings.max_encoded_percent || 75) + '%';
            
            if (document.getElementById('max-encoded-mode')) {
                document.getElementById('max-encoded-mode').value = settings.max_encoded_mode || 'full';
            }
            
            document.getElementById('svt-preset').value = settings.svt_preset || 2;
            document.getElementById('svt-preset-value').textContent = settings.svt_preset || 2;
            
            document.getElementById('svt-workers').value = settings.svt_worker_count || 1;
            document.getElementById('svt-workers-value').textContent = settings.svt_worker_count || 1;
            
            document.getElementById('nvenc-enabled').checked = settings.nvenc_enabled !== false;
            document.getElementById('nvenc-workers').value = settings.nvenc_worker_count || 1;
            document.getElementById('nvenc-workers-value').textContent = settings.nvenc_worker_count || 1;
            
            document.getElementById('crf-increment').value = settings.crf_increment || 1;
            document.getElementById('crf-increment-value').textContent = settings.crf_increment || 1;
            
            document.getElementById('resize-enabled').checked = !!settings.resize_enabled;
            document.getElementById('resize-height').value = settings.resize_height || 1080;
            document.getElementById('resize-height-value').textContent = (settings.resize_height || 1080) + 'p';
            document.getElementById('resize-height-group').style.display = settings.resize_enabled ? 'block' : 'none';
            
            document.getElementById('audio-compression').checked = !!settings.audio_compression_enabled;
            
            // Always sync paths from Windows GUI
            document.getElementById('source-path').value = settings.source_path || '';
            document.getElementById('dest-path').value = settings.dest_path || '';
        }
        
        // =====================================================================
        // Pagination State & Infinite Scroll
        // =====================================================================
        
        window.videoState = { currentPage: 0, totalPages: 1, total: 0, isLoading: false };
        
        function loadMoreVideos() {
            if (window.videoState.isLoading) return;
            if (window.videoState.currentPage >= window.videoState.totalPages - 1) return;
            
            window.videoState.isLoading = true;
            const nextPage = window.videoState.currentPage + 1;
            
            fetch(`/api/videos?page=${nextPage}&page_size=1000`)
                .then(res => res.json())
                .then(data => {
                    updateVideoList(data);
                    window.videoState.isLoading = false;
                })
                .catch(e => {
                    console.error('Load more error:', e);
                    window.videoState.isLoading = false;
                });
        }
        
        // Infinite scroll listener - load more when near bottom
        document.addEventListener('scroll', () => {
            const scrollPos = window.innerHeight + window.scrollY;
            const threshold = document.body.offsetHeight - 500; // 500px before bottom
            
            if (scrollPos >= threshold) {
                loadMoreVideos();
            }
        });
        
        // Also check table container scroll (if table is in a scrollable container)
        const tableContainer = document.querySelector('.video-table-container');
        if (tableContainer) {
            tableContainer.addEventListener('scroll', () => {
                const scrollPos = tableContainer.scrollTop + tableContainer.clientHeight;
                const threshold = tableContainer.scrollHeight - 300;
                
                if (scrollPos >= threshold) {
                    loadMoreVideos();
                }
            });
        }
        
        // =====================================================================
        // API Calls
        // =====================================================================
        
        async function fetchStatus() {
            try {
                const res = await fetch('/api/status');
                const data = await res.json();
                recordContactSuccess();
                updateStatus(data);
            } catch (e) {
                console.error('Status fetch error:', e);
            }
        }
        
        async function fetchVideos() {
            try {
                window.videoState = { currentPage: 0, totalPages: 1, total: 0, isLoading: true };
                const res = await fetch('/api/videos?page=0&page_size=1000');
                const data = await res.json();
                recordContactSuccess();
                updateVideoList(data);
            } catch (e) {
                console.error('Videos fetch error:', e);
                window.videoState.isLoading = false;
            }
        }
        
        async function fetchSettings() {
            try {
                const res = await fetch('/api/settings');
                const data = await res.json();
                recordContactSuccess();
                updateSettings(data);
            } catch (e) {
                console.error('Settings fetch error:', e);
            }
        }
        
        async function postAction(endpoint, data = {}) {
            try {
                const res = await fetch(endpoint, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(data)
                });
                return await res.json();
            } catch (e) {
                console.error('Action error:', e);
                return { success: false, message: e.message };
            }
        }
        
        // =====================================================================
        // Event Handlers
        // =====================================================================
        
        document.getElementById('btn-start').addEventListener('click', async () => {
            const result = await postAction('/api/action/start');
            if (result.success) fetchStatus();
        });
        
        document.getElementById('btn-stop').addEventListener('click', async () => {
            const result = await postAction('/api/action/stop');
            if (result.success) fetchStatus();
        });
        
        document.getElementById('btn-stop-immediate').addEventListener('click', async () => {
            if (confirm(\'''' + self._t('http_confirm_stop_immediate') + '''\')) {
                const result = await postAction('/api/action/stop_immediate');
                if (result.success) fetchStatus();
            }
        });
        
        document.getElementById('btn-load').addEventListener('click', async () => {
            const source = document.getElementById('source-path').value;
            const dest = document.getElementById('dest-path').value;
            const result = await postAction('/api/action/load', { source_path: source, dest_path: dest });
            if (result.success) {
                setTimeout(fetchVideos, 2000);
                setTimeout(fetchStatus, 2000);
            }
        });
        
        document.getElementById('btn-save-settings').addEventListener('click', async () => {
            const settings = {
                min_vmaf: parseFloat(document.getElementById('min-vmaf').value),
                vmaf_step: parseFloat(document.getElementById('vmaf-step').value),
                max_encoded_percent: parseInt(document.getElementById('max-encoded').value),
                max_encoded_mode: document.getElementById('max-encoded-mode').value,
                svt_preset: parseInt(document.getElementById('svt-preset').value),
                svt_worker_count: parseInt(document.getElementById('svt-workers').value),
                crf_increment: parseInt(document.getElementById('crf-increment').value),
                nvenc_enabled: document.getElementById('nvenc-enabled').checked,
                nvenc_worker_count: parseInt(document.getElementById('nvenc-workers').value),
                resize_enabled: document.getElementById('resize-enabled').checked,
                resize_height: parseInt(document.getElementById('resize-height').value),
                audio_compression_enabled: document.getElementById('audio-compression').checked
            };
            const result = await postAction('/api/settings', settings);
            if (result.encoding_warning) {
                alert(\'''' + self._t('http_saved_with_warning').replace('\n', '\\n') + '''\'.replace('{warning}', result.encoding_warning));
            } else {
                alert(\'''' + self._t('http_settings_saved') + '''\');
            }
        });
        
        // Slider value updates - INSTANT sync to Windows GUI
        let sliderDebounceTimers = {};
        
        document.querySelectorAll('input[type="range"]').forEach(slider => {
            slider.addEventListener('input', (e) => {
                const id = e.target.id;
                const value = e.target.value;
                const valueEl = document.getElementById(id + '-value');
                
                // Update local display
                if (valueEl) {
                    if (id === 'min-vmaf') {
                        valueEl.textContent = parseFloat(value).toFixed(2);
                    } else if (id === 'vmaf-step') {
                        valueEl.textContent = parseFloat(value).toFixed(2);
                    } else if (id === 'max-encoded') {
                        valueEl.textContent = value + '%';
                    } else if (id === 'resize-height') {
                        valueEl.textContent = value + 'p';
                    } else {
                        valueEl.textContent = value;
                    }
                }
                
                // Debounce: wait 100ms after last input before sending to server
                clearTimeout(sliderDebounceTimers[id]);
                sliderDebounceTimers[id] = setTimeout(() => {
                    syncSingleSetting(id, value);
                }, 100);
            });
        });
        
        // Checkbox instant sync
        ['nvenc-enabled', 'resize-enabled', 'audio-compression'].forEach(id => {
            document.getElementById(id).addEventListener('change', (e) => {
                if (id === 'resize-enabled') {
                    document.getElementById('resize-height-group').style.display = e.target.checked ? 'block' : 'none';
                }
                syncSingleSetting(id, e.target.checked);
            });
        });
        
        // Max encoded mode select sync
        const maxEncodedModeSelect = document.getElementById('max-encoded-mode');
        if (maxEncodedModeSelect) {
            maxEncodedModeSelect.addEventListener('change', (e) => {
                syncSingleSetting('max-encoded-mode', e.target.value);
            });
        }
        
        // Sync single setting to Windows GUI
        async function syncSingleSetting(id, value) {
            const mapping = {
                'min-vmaf': 'min_vmaf',
                'vmaf-step': 'vmaf_step',
                'max-encoded': 'max_encoded_percent',
                'max-encoded-mode': 'max_encoded_mode',
                'svt-preset': 'svt_preset',
                'svt-workers': 'svt_worker_count',
                'crf-increment': 'crf_increment',
                'nvenc-workers': 'nvenc_worker_count',
                'resize-height': 'resize_height',
                'nvenc-enabled': 'nvenc_enabled',
                'resize-enabled': 'resize_enabled',
                'audio-compression': 'audio_compression_enabled'
            };
            
            const key = mapping[id];
            if (!key) return;
            
            let sendValue = value;
            if (['min-vmaf', 'vmaf-step'].includes(id)) {
                sendValue = parseFloat(value);
            } else if (['max-encoded', 'svt-preset', 'svt-workers', 'crf-increment', 'nvenc-workers', 'resize-height'].includes(id)) {
                sendValue = parseInt(value);
            } else if (id === 'max-encoded-mode') {
                sendValue = value;  // Keep as string ('full' or 'video')
            } else {
                sendValue = !!value;
            }
            
            const result = await postAction('/api/settings', { [key]: sendValue });
            
            // Show warning toast if encoding is in progress
            if (result.encoding_warning && !window.encodingWarningShown) {
                window.encodingWarningShown = true;
                showEncodingWarning(result.encoding_warning);
                // Reset after 5 seconds to allow showing again
                setTimeout(() => { window.encodingWarningShown = false; }, 5000);
            }
        }
        
        // Toast notification for encoding warning
        function showEncodingWarning(message) {
            // Create toast element if not exists
            let toast = document.getElementById('encoding-warning-toast');
            if (!toast) {
                toast = document.createElement('div');
                toast.id = 'encoding-warning-toast';
                toast.style.cssText = 'position:fixed;top:20px;right:20px;background:#ff9800;color:#fff;padding:15px 25px;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.3);z-index:10000;font-weight:500;max-width:400px;animation:slideIn 0.3s ease;';
                document.body.appendChild(toast);
            }
            toast.textContent = toWebDecorativeText('[WARN] ' + message);
            toast.style.display = 'block';
            
            // Auto-hide after 4 seconds
            setTimeout(() => {
                toast.style.display = 'none';
            }, 4000);
        }
        
        // =====================================================================
        // Connection Monitoring (Stuck detection)
        // =====================================================================
        let lastSuccessfulContact = Date.now();
        let connectionStuck = false;
        let lastStuckNotificationTime = 0;

        function recordContactSuccess() {
            lastSuccessfulContact = Date.now();
            if (connectionStuck) {
                connectionStuck = false;
                const banner = document.getElementById('connection-lost-banner');
                if (banner) banner.style.display = 'none';
            }
        }

        function checkConnection() {
            const now = Date.now();
            const timeSinceLastContact = now - lastSuccessfulContact;

            // Detect if connection is lost (no successful heartbeat for 30s)
            // Increased from 15s to reduce false positives
            if (timeSinceLastContact > 30000) {
                connectionStuck = true;
            }

            if (connectionStuck) {
                // If stuck, notify every 1 minute (60,000 ms)
                if (now - lastStuckNotificationTime > 60000) {
                    showStuckNotification();
                    lastStuckNotificationTime = now;
                }
            }
        }

        function showStuckNotification() {
            const banner = document.getElementById('connection-lost-banner');
            if (banner) {
                banner.style.display = 'flex';
                playAlertSound();
                // 10 second notification as requested
                setTimeout(() => {
                    banner.style.display = 'none';
                }, 10000);
            }
        }

        // Audio Context Management
        let globalAudioCtx = null;

        function initAudio() {
            if (!globalAudioCtx) {
                try {
                    globalAudioCtx = new (window.AudioContext || window.webkitAudioContext)();
                } catch (e) {
                    console.warn('Web Audio API not supported');
                }
            }
            // Resume if suspended (needs user gesture)
            if (globalAudioCtx && globalAudioCtx.state === 'suspended') {
                globalAudioCtx.resume().then(() => {
                    console.log('AudioContext resumed successfully');
                }).catch(e => console.warn('AudioContext resume failed:', e));
            }
        }

        // Enable audio on first interaction to satisfy browser Autoplay Policy
        document.addEventListener('click', initAudio, { once: true });
        document.addEventListener('touchstart', initAudio, { once: true });
        document.addEventListener('keydown', initAudio, { once: true });

        function playAlertSound() {
            try {
                if (!globalAudioCtx) initAudio();
                if (!globalAudioCtx) return;

                // Try to resume again just in case
                if (globalAudioCtx.state === 'suspended') {
                    globalAudioCtx.resume();
                }
                
                const oscillator = globalAudioCtx.createOscillator();
                const gainNode = globalAudioCtx.createGain();

                oscillator.connect(gainNode);
                gainNode.connect(globalAudioCtx.destination);

                oscillator.type = 'sine';
                oscillator.frequency.setValueAtTime(880, globalAudioCtx.currentTime); // A5 note
                gainNode.gain.setValueAtTime(0.1, globalAudioCtx.currentTime);
                gainNode.gain.exponentialRampToValueAtTime(0.01, globalAudioCtx.currentTime + 0.5);

                oscillator.start();
                oscillator.stop(globalAudioCtx.currentTime + 0.5);
            } catch (e) {
                console.warn('Audio feedback failed:', e);
            }
        }

        // Check connection state every second
        setInterval(checkConnection, 1000);

        // =====================================================================
        // Initialization & Auto-refresh
        // =====================================================================
        
        let lastSettingsJson = '';
        
        async function fetchAndSyncSettings() {
            try {
                const res = await fetch('/api/settings');
                const data = await res.json();
                recordContactSuccess();
                const newJson = JSON.stringify(data);
                
                // Only update UI if settings actually changed (from Windows GUI)
                if (newJson !== lastSettingsJson) {
                    lastSettingsJson = newJson;
                    updateSettings(data);
                }
            } catch (e) {
                console.error('Settings fetch error:', e);
            }
        }
        
        async function init() {
            await fetchAndSyncSettings();
            
            // Set language selector based on loaded settings
            if (currentSettings.language) {
                const langSel = document.getElementById('lang-selector');
                if (langSel) langSel.value = currentSettings.language;
            }
            // Bind language change event
            const langSel = document.getElementById('lang-selector');
            if (langSel) {
                langSel.addEventListener('change', async (e) => {
                    const newLang = e.target.value;
                    console.log('Language change requested:', newLang);
                    try {
                        const res = await postAction('/api/action/language', { language: newLang });
                        if (res && res.success) {
                            console.log('Language changed successfully, refreshing...');
                            window.location.reload();
                        } else {
                            console.error('Language change failed:', res);
                            alert('Failed to change language: ' + (res.message || 'Unknown error'));
                        }
                    } catch (err) {
                        console.error('Language change error:', err);
                        alert('Error changing language: ' + err.message);
                    }
                });
            }
            
            await fetchStatus();
            await fetchVideos();
        }
        
        // Auto-refresh - includes settings for Windows GUI -> Web sync
        setInterval(() => {
            fetchStatus();
            fetchVideos();
            fetchAndSyncSettings();  // Sync settings from Windows GUI
        }, 1000);  // Faster refresh: 1 second
        
        // Initial load
        init();
        
        // =====================================================================
        // Mobile Sticky Header Logic
        // =====================================================================
        
        function setupMobileStickyHeader() {
            const videoHeader = document.querySelector('.video-header');
            const settingsPanel = document.querySelector('.settings-panel');
            const mainHeader = document.querySelector('.header');
            
            if (!videoHeader || !settingsPanel) return;
            
            let lastScrollY = 0;
            let ticking = false;
            
            function updateStickyState() {
                const headerRect = videoHeader.getBoundingClientRect();
                
                // Check if we're in mobile view (window width <= 768px)
                if (window.innerWidth > 768) {
                    videoHeader.classList.remove('is-sticky');
                    videoHeader.style.top = '';  // Reset inline style
                    return;
                }
                
                // Dynamically calculate main header height and set video-header top offset
                if (mainHeader) {
                    const mainHeaderHeight = mainHeader.offsetHeight;
                    videoHeader.style.top = mainHeaderHeight + 'px';
                }
                
                // If the header is at top of viewport (stuck), add shadow
                if (headerRect.top <= 1) {
                    videoHeader.classList.add('is-sticky');
                } else {
                    videoHeader.classList.remove('is-sticky');
                }
                
                ticking = false;
            }
            
            window.addEventListener('scroll', () => {
                if (!ticking) {
                    requestAnimationFrame(updateStickyState);
                    ticking = true;
                }
            }, { passive: true });
            
            // Also check on resize
            window.addEventListener('resize', updateStickyState, { passive: true });
            
            // Initial check
            updateStickyState();
        }
        
        // Initialize mobile sticky header
        setupMobileStickyHeader();

        
        // =====================================================================
        // Column Resizing Logic
        // =====================================================================

        function makeTableResizable() {
            const table = document.querySelector('.video-table');
            if (!table) return;

            const cols = table.querySelectorAll('th');
            [].forEach.call(cols, function (col) {
                // Find the resizer div inside the th
                const resizer = col.querySelector('.resizer');
                if (!resizer) return;

                createResizableColumn(col, resizer);
            });
        }

        function createResizableColumn(col, resizer) {
            let x = 0;
            let w = 0;

            const mouseDownHandler = function (e) {
                // Prevent text selection
                e.preventDefault();
                
                x = e.clientX;
                const styles = window.getComputedStyle(col);
                w = parseInt(styles.width, 10);

                document.addEventListener('mousemove', mouseMoveHandler);
                document.addEventListener('mouseup', mouseUpHandler);

                resizer.classList.add('resizing');
                document.body.style.cursor = 'col-resize';
            };

            const mouseMoveHandler = function (e) {
                const dx = e.clientX - x;
                // Update width directly
                col.style.width = `${w + dx}px`;
            };

            const mouseUpHandler = function () {
                document.removeEventListener('mousemove', mouseMoveHandler);
                document.removeEventListener('mouseup', mouseUpHandler);

                resizer.classList.remove('resizing');
                document.body.style.cursor = '';
            };

            resizer.addEventListener('mousedown', mouseDownHandler);
        }
        
        // Initialize resizable columns
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', makeTableResizable);
        } else {
            makeTableResizable();
        }
    </script>
</body>
</html>'''

    def _get_css(self):
        """Return CSS styles for the web interface."""
        return '''
/* =========================================================================
   AV1 Batch Encoder - Web Interface Styles
   Modern, dark theme with glassmorphism effects
   ========================================================================= */

:root {
    --bg-dark: #0d1117;
    --bg-card: #161b22;
    --bg-card-hover: #1f2937;
    --border-color: #30363d;
    --text-primary: #e6edf3;
    --text-secondary: #8b949e;
    --accent-blue: #58a6ff;
    --accent-green: #3fb950;
    --accent-yellow: #d29922;
    --accent-red: #f85149;
    --accent-purple: #a371f7;
    --gradient-start: #667eea;
    --gradient-end: #764ba2;
}

/* Connection Lost Banner */
.connection-lost-banner {
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    height: 50px;
    background: linear-gradient(90deg, var(--accent-red), #991b1b);
    color: white;
    display: flex;
    align-items: center;
    justify-content: center;
    font-weight: 700;
    font-size: 1.1rem;
    z-index: 10000;
    box-shadow: 0 4px 20px rgba(0,0,0,0.5);
    animation: slideDownIn 0.5s cubic-bezier(0.18, 0.89, 0.32, 1.28);
    backdrop-filter: blur(10px);
    border-bottom: 2px solid rgba(255,255,255,0.2);
}

@keyframes slideDownIn {
    from { transform: translateY(-100%); opacity: 0; }
    to { transform: translateY(0); opacity: 1; }
}

* {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
}

body {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: linear-gradient(135deg, var(--bg-dark) 0%, #1a1f35 100%);
    color: var(--text-primary);
    height: 100vh;
    overflow: hidden;  /* Desktop: no body scroll */
    line-height: 1.5;
}

.app-container {
    display: flex;
    flex-direction: column;
    height: 100vh;
    overflow: hidden;
}

/* Header */
.header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 1rem 2rem;
    background: rgba(22, 27, 34, 0.8);
    backdrop-filter: blur(10px);
    border-bottom: 1px solid var(--border-color);
    position: sticky;
    top: 0;
    z-index: 100;
}

.header-right {
    display: flex;
    align-items: center;
    gap: 1rem;
}

.logo {
    font-size: 1.5rem;
    font-weight: 700;
    background: linear-gradient(135deg, var(--gradient-start), var(--gradient-end));
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
}

.status-badge {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.5rem 1rem;
    border-radius: 20px;
    font-weight: 600;
    font-size: 0.85rem;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

.status-badge.idle {
    background: rgba(139, 148, 158, 0.2);
    color: var(--text-secondary);
}

.status-badge.running {
    background: rgba(63, 185, 80, 0.2);
    color: var(--accent-green);
    animation: pulse 2s infinite;
}

.status-badge.loading {
    background: rgba(88, 166, 255, 0.2);
    color: var(--accent-blue);
}

.status-badge.stopping {
    background: rgba(210, 153, 34, 0.2);
    color: var(--accent-yellow);
}

.status-dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: currentColor;
}

@keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.7; }
}

.lang-selector {
    background: rgba(22, 27, 34, 0.8);
    color: var(--text-primary);
    border: 1px solid var(--border-color);
    border-radius: 6px;
    padding: 6px 12px;
    cursor: pointer;
    font-size: 0.85rem;
    font-weight: 500;
}

/* Loading Progress Panel */
.loading-details {
    background: linear-gradient(135deg, rgba(88, 166, 255, 0.1) 0%, rgba(102, 126, 234, 0.15) 100%);
    border: 1px solid rgba(88, 166, 255, 0.3);
    border-radius: 8px;
    padding: 1rem 2rem;
    margin: 0 2rem 1rem 2rem;
    animation: slideDown 0.3s ease-out;
}

@keyframes slideDown {
    from {
        opacity: 0;
        transform: translateY(-10px);
    }
    to {
        opacity: 1;
        transform: translateY(0);
    }
}

.loading-progress-bar {
    width: 100%;
    height: 8px;
    background: rgba(255, 255, 255, 0.1);
    border-radius: 4px;
    overflow: hidden;
    margin-bottom: 0.75rem;
}

.loading-progress-fill {
    height: 100%;
    background: linear-gradient(90deg, var(--accent-blue), var(--accent-purple));
    border-radius: 4px;
    transition: width 0.3s ease-out;
    position: relative;
}

.loading-progress-fill::after {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: linear-gradient(90deg, transparent, rgba(255,255,255,0.3), transparent);
    animation: shimmer 1.5s infinite;
}

@keyframes shimmer {
    0% { transform: translateX(-100%); }
    100% { transform: translateX(100%); }
}

.loading-progress-text {
    font-size: 0.95rem;
    font-weight: 500;
    color: var(--accent-blue);
    margin-bottom: 0.25rem;
}

.loading-elapsed {
    font-size: 0.8rem;
    color: var(--text-secondary);
}

/* Main Content */
.main-content {
    display: flex;
    flex: 1;
    gap: 1.5rem;
    padding: 1.5rem;
    height: calc(100vh - 80px);  /* Fixed height for flex children */
    overflow: hidden;  /* Children handle their own scrolling */
}

/* Settings Panel */
.settings-panel {
    width: 320px;
    flex-shrink: 0;
    display: flex;
    flex-direction: column;
    gap: 1rem;
    overflow-y: auto;
}

.panel-section {
    background: var(--bg-card);
    border: 1px solid var(--border-color);
    border-radius: 12px;
    padding: 1.25rem;
}

.panel-section h3 {
    font-size: 0.9rem;
    font-weight: 600;
    color: var(--text-secondary);
    margin-bottom: 1rem;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

.form-group {
    margin-bottom: 1rem;
}

.form-group label {
    display: block;
    font-size: 0.85rem;
    color: var(--text-secondary);
    margin-bottom: 0.4rem;
}

.form-input {
    width: 100%;
    padding: 0.6rem 0.8rem;
    background: var(--bg-dark);
    border: 1px solid var(--border-color);
    border-radius: 8px;
    color: var(--text-primary);
    font-size: 0.9rem;
    transition: border-color 0.2s, box-shadow 0.2s;
}

.form-input:focus {
    outline: none;
    border-color: var(--accent-blue);
    box-shadow: 0 0 0 3px rgba(88, 166, 255, 0.15);
}

.slider-container {
    display: flex;
    align-items: center;
    gap: 0.75rem;
}

.slider-container input[type="range"] {
    flex: 1;
    -webkit-appearance: none;
    height: 6px;
    background: var(--border-color);
    border-radius: 3px;
    cursor: pointer;
}

.slider-container input[type="range"]::-webkit-slider-thumb {
    -webkit-appearance: none;
    width: 16px;
    height: 16px;
    background: var(--accent-blue);
    border-radius: 50%;
    cursor: pointer;
    transition: transform 0.2s;
}

.slider-container input[type="range"]::-webkit-slider-thumb:hover {
    transform: scale(1.2);
}

.slider-value {
    min-width: 50px;
    text-align: right;
    font-weight: 600;
    color: var(--accent-blue);
    font-size: 0.9rem;
}

.mode-select-container {
    margin-bottom: 0.5rem;
}

.mode-select {
    width: 100%;
    padding: 0.4rem 0.6rem;
    background: var(--bg-dark);
    border: 1px solid var(--border-color);
    border-radius: 6px;
    color: var(--text-primary);
    font-size: 0.85rem;
    cursor: pointer;
}

.mode-select:focus {
    outline: none;
    border-color: var(--accent-blue);
}

.checkbox-group {
    margin-bottom: 0.75rem;
}

.checkbox-label {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    cursor: pointer;
    font-size: 0.9rem;
}

.checkbox-label input[type="checkbox"] {
    width: 16px;
    height: 16px;
    accent-color: var(--accent-blue);
}

/* Buttons */
.btn {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 0.5rem;
    padding: 0.6rem 1.2rem;
    border: none;
    border-radius: 8px;
    font-size: 0.9rem;
    font-weight: 500;
    cursor: pointer;
    transition: all 0.2s;
}

.btn:disabled {
    opacity: 0.5;
    cursor: not-allowed;
}

.btn-primary {
    background: linear-gradient(135deg, var(--gradient-start), var(--gradient-end));
    color: white;
}

.btn-primary:hover:not(:disabled) {
    transform: translateY(-1px);
    box-shadow: 0 4px 12px rgba(102, 126, 234, 0.4);
}

.btn-secondary {
    background: var(--accent-blue);
    color: white;
}

.btn-warning {
    background: var(--accent-yellow);
    color: #000;
}

.btn-danger {
    background: var(--accent-red);
    color: white;
}

.btn-outline {
    background: transparent;
    border: 1px solid var(--border-color);
    color: var(--text-primary);
}

.btn-outline:hover:not(:disabled) {
    background: var(--bg-card-hover);
}

.btn-full {
    width: 100%;
}

/* Video Panel */
.video-panel {
    flex: 1;
    display: flex;
    flex-direction: column;
    background: var(--bg-card);
    border: 1px solid var(--border-color);
    border-radius: 12px;
    overflow: hidden;
}

.video-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 1rem 1.25rem;
    border-bottom: 1px solid var(--border-color);
    flex-wrap: wrap;
    gap: 1rem;
}

.video-stats {
    display: flex;
    gap: 1.5rem;
}

.stat-item {
    text-align: center;
}

.stat-value {
    display: block;
    font-size: 1.5rem;
    font-weight: 700;
    color: var(--text-primary);
}

.stat-value.stat-completed { color: var(--accent-green); }
.stat-value.stat-encoding { color: var(--accent-blue); }
.stat-value.stat-pending { color: var(--accent-yellow); }

.stat-label {
    font-size: 0.75rem;
    color: var(--text-secondary);
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

.video-actions {
    display: flex;
    gap: 0.5rem;
}

/* Video Table */
.video-table-container {
    flex: 1;
    overflow-y: auto;
    overflow-x: hidden;
    position: relative;
}

.video-summary {
    display: flex;
    gap: 1.5rem;
    padding: 0.75rem 1rem;
    background: var(--bg-dark);
    width: 100%;  /* Ensure it covers full width */
    box-sizing: border-box;
    /* Remove bottom margin and radius to prevent content showing through */
    margin-bottom: 0;
    border-radius: 8px 8px 0 0; /* Only top radius */
    border-bottom: 1px solid var(--border-color);
    font-size: 0.9rem;
    font-weight: 500;
    position: sticky;
    top: 0;
    z-index: 60;  /* Above thead (50) */
    box-shadow: 0 4px 12px rgba(0,0,0,0.2); /* Extra shadow for better separation */
}

.video-summary span {
    padding: 0.25rem 0.75rem;
    border-radius: 4px;
}

.summary-total { color: var(--text-primary); }
.summary-pending { color: #64b5f6; background: rgba(100, 181, 246, 0.15); }
.summary-encoding { color: #ffd54f; background: rgba(255, 213, 79, 0.15); }
.summary-completed { color: #81c784; background: rgba(129, 199, 132, 0.15); }
.summary-failed { color: #e57373; background: rgba(229, 115, 115, 0.15); }

/* Size summary styles */
.summary-divider { color: var(--border-color); background: none !important; padding: 0 !important; }
.summary-orig-size { color: var(--text-secondary); background: rgba(139, 148, 158, 0.1); }
.summary-new-size { color: var(--accent-purple); background: rgba(163, 113, 247, 0.15); }
.summary-savings { font-weight: 600; }
.summary-savings.size-reduced { color: #4caf50; background: rgba(76, 175, 80, 0.2); }
.summary-savings.size-increased { color: #ff9800; background: rgba(255, 152, 0, 0.2); }

.video-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.85rem;
    table-layout: fixed;  /* CRITICAL: Prevents column width jumping */
}

/* Desktop: Sticky table header - below the summary (~45px) */
.video-table thead {
    position: sticky;
    top: 45px;  /* Below video-summary */
    z-index: 50;
}

.video-table th {
    background: var(--bg-dark);
    padding: 0.75rem 0.5rem;
    font-weight: 600;
    color: var(--text-secondary);
    border-bottom: 1px solid var(--border-color);
    white-space: nowrap;
    overflow: visible; /* Allow resizer visibility on hover */
    text-overflow: ellipsis;
}

.video-table td {
    padding: 0.6rem 0.5rem;
    border-bottom: 1px solid var(--border-color);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}

.video-row:hover {
    background: var(--bg-card-hover);
}

.video-row.completed td:nth-child(3) { color: var(--accent-green); }
.video-row.encoding td:nth-child(3) { color: var(--accent-blue); }
.video-row.failed td:nth-child(3) { color: var(--accent-red); }
.video-row.pending td:nth-child(3) { color: var(--text-secondary); }

/* Fixed column widths to prevent jumping during refresh */
/* Alignment fixes using specific selectors for both th and td */
.col-order, th.col-order { width: 45px; text-align: center; }
.col-denoise, th.col-denoise { width: 80px; text-align: center; }
.col-name, th.col-name { width: 25%; min-width: 150px; max-width: 300px; text-align: left; }
.col-status, th.col-status { width: 200px; text-align: left; }
.col-cq, th.col-cq { width: 50px; text-align: center; }
.col-vmaf, th.col-vmaf { width: 60px; text-align: center; }
.col-progress, th.col-progress { width: 100px; text-align: center; }
.col-orig, th.col-orig { width: 90px; text-align: right; }
.col-new, th.col-new { width: 90px; text-align: right; }
.col-change, th.col-change { width: 80px; text-align: right; }
.col-eta, th.col-eta { width: 150px; text-align: center; }

/* Column Resizer Styles */
.resizer {
    position: absolute;
    top: 0;
    right: 0;
    width: 6px;
    cursor: col-resize;
    user-select: none;
    height: 100%;
    z-index: 10;
    /* Invisible by default, defined area for grabbing */
    opacity: 0; 
    transition: background-color 0.2s;
}

.resizer:hover, .resizing {
    background-color: var(--accent-blue);
    opacity: 1;
}



.empty-row td {
    text-align: center;
    padding: 3rem;
    color: var(--text-secondary);
}

/* Footer */
.footer {
    display: flex;
    justify-content: space-between;
    padding: 0.75rem 2rem;
    background: rgba(22, 27, 34, 0.8);
    border-top: 1px solid var(--border-color);
    font-size: 0.8rem;
    color: var(--text-secondary);
}

/* Responsive */
@media (max-width: 1024px) {
    .main-content {
        flex-direction: column;
        max-height: none;
    }
    .settings-panel {
        width: 100%;
        flex-direction: row;
        flex-wrap: wrap;
    }
    .panel-section {
        flex: 1;
        min-width: 280px;
    }
}

/* Desktop/Mobile visibility */
.mobile-only { display: none !important; }
.desktop-only { display: table; }

/* =========================================================================
   Mobile Card View Styles
   ========================================================================= */

/* NOTE: .video-cards inherits display:none from .mobile-only on desktop */
/* The flex display is set only in the mobile media query below */
.video-cards {
    flex-direction: column;
    gap: 0.75rem;
    padding: 0.5rem;
}

.video-card {
    background: var(--bg-card);
    border: 1px solid var(--border-color);
    border-radius: 12px;
    padding: 1rem;
    transition: transform 0.2s, box-shadow 0.2s;
}

.video-card:active {
    transform: scale(0.98);
}

.video-card.completed {
    border-left: 4px solid var(--accent-green);
}

.video-card.encoding {
    border-left: 4px solid var(--accent-blue);
    animation: cardPulse 2s infinite;
}

.video-card.failed {
    border-left: 4px solid var(--accent-red);
}

.video-card.pending {
    border-left: 4px solid var(--text-secondary);
}

@keyframes cardPulse {
    0%, 100% { box-shadow: 0 0 0 0 rgba(88, 166, 255, 0); }
    50% { box-shadow: 0 0 12px 2px rgba(88, 166, 255, 0.3); }
}

.card-header {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    margin-bottom: 0.75rem;
}

.card-order {
    background: var(--bg-dark);
    color: var(--text-secondary);
    padding: 0.2rem 0.5rem;
    border-radius: 4px;
    font-size: 0.75rem;
    font-weight: 600;
    flex-shrink: 0;
}

.denoise-badge {
    background: var(--accent-blue);
    color: #fff;
    padding: 0.2rem 0.4rem;
    border-radius: 4px;
    font-size: 0.8rem;
    font-weight: 600;
    flex-shrink: 0;
    letter-spacing: 1px;
}

.card-name {
    font-weight: 600;
    font-size: 0.95rem;
    color: var(--text-primary);
    word-break: break-word;
    line-height: 1.3;
}

.card-status {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.5rem 0.75rem;
    background: var(--bg-dark);
    border-radius: 8px;
    margin-bottom: 0.75rem;
}

.status-icon {
    font-size: 1.1rem;
}

.status-text {
    font-size: 0.9rem;
    font-weight: 500;
    color: var(--text-primary);
    word-break: break-word;
}

.video-card.completed .status-text { color: var(--accent-green); }
.video-card.encoding .status-text { color: var(--accent-blue); }
.video-card.failed .status-text { color: var(--accent-red); }

.card-metrics {
    display: flex;
    justify-content: space-around;
    gap: 0.5rem;
    margin-bottom: 0.75rem;
}

.metric {
    display: flex;
    flex-direction: column;
    align-items: center;
    flex: 1;
    padding: 0.5rem;
    background: var(--bg-dark);
    border-radius: 8px;
}

.metric-label {
    font-size: 0.7rem;
    color: var(--text-secondary);
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

.metric-value {
    font-size: 1rem;
    font-weight: 600;
    color: var(--accent-blue);
}

.card-sizes {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 0.5rem;
    padding: 0.5rem;
    background: linear-gradient(135deg, rgba(102, 126, 234, 0.1), rgba(118, 75, 162, 0.1));
    border-radius: 8px;
    margin-bottom: 0.5rem;
    flex-wrap: wrap;
}

.size-orig, .size-new {
    font-weight: 500;
    color: var(--text-primary);
}

.size-arrow {
    color: var(--accent-purple);
}

.size-change {
    font-weight: 600;
    padding: 0.2rem 0.5rem;
    border-radius: 4px;
}

.size-change.size-reduced {
    color: var(--accent-green);
    background: rgba(63, 185, 80, 0.15);
}

.size-change.size-increased {
    color: var(--accent-red);
    background: rgba(248, 81, 73, 0.15);
}

.card-eta {
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-size: 0.85rem;
}

.eta-label {
    color: var(--text-secondary);
}

.eta-value {
    font-weight: 500;
    color: var(--accent-purple);
}

.empty-card {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 3rem 1rem;
    text-align: center;
    color: var(--text-secondary);
}

.empty-card span {
    font-size: 3rem;
    margin-bottom: 1rem;
}

.empty-card p {
    margin: 0.25rem 0;
}

.empty-card .hint {
    font-size: 0.85rem;
    opacity: 0.7;
}

/* =========================================================================
   Mobile Responsive (< 768px)
   ========================================================================= */

@media (max-width: 768px) {
    /* Show cards, hide table */
    .mobile-only { display: flex !important; }
    .desktop-only { display: none !important; }
    
    /* Mobile: Allow natural scrolling */
    body {
        height: auto;
        overflow: auto;
    }
    
    .app-container {
        height: auto;
        overflow: visible;
    }
    
    /* Main header - sticky at top with highest z-index */
    .header {
        padding: 0.75rem 1rem;
        flex-wrap: wrap;
        gap: 0.5rem;
        position: sticky;
        top: 0;
        z-index: 300;
    }
    
    .logo {
        font-size: 1.2rem;
    }
    
    .status-badge {
        padding: 0.4rem 0.75rem;
        font-size: 0.75rem;
    }
    
    /* Loading details mobile */
    .loading-details {
        margin: 0 1rem 1rem 1rem;
        padding: 0.75rem 1rem;
    }
    
    /* Main content stacks vertically - allow natural scroll */
    .main-content {
        padding: 0.75rem;
        gap: 0.75rem;
        height: auto;
        max-height: none;
        overflow: visible;
        flex-direction: column;
    }
    
    /* Settings panel horizontal scroll or collapse */
    .settings-panel {
        flex-direction: column;
        gap: 0.75rem;
        width: 100%;
    }
    
    .panel-section {
        padding: 1rem;
        min-width: auto;
    }
    
    .panel-section h3 {
        font-size: 0.85rem;
    }
    
    /* Video panel needs overflow visible for sticky to work */
    .video-panel {
        overflow: visible;
    }
    
    /* Video table container - no max-height on mobile */
    .video-table-container {
        max-height: none;
        overflow: visible;
    }
    
    /* Video header mobile - sticky below main header (~50px) */
    .video-header {
        padding: 0.75rem 1rem;
        flex-direction: column;
        align-items: stretch;
        position: sticky;
        top: 50px;  /* Below main header */
        z-index: 200;
        background: linear-gradient(135deg, rgba(26, 26, 46, 0.98) 0%, rgba(22, 33, 62, 0.98) 100%);
        backdrop-filter: blur(10px);
        -webkit-backdrop-filter: blur(10px);
        transition: box-shadow 0.3s ease;
        border-radius: 12px 12px 0 0;
    }
    
    .video-header.is-sticky {
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.4);
    }
    
    .video-stats {
        justify-content: space-around;
        gap: 0.5rem;
    }
    
    .stat-value {
        font-size: 1.25rem;
    }
    
    .stat-label {
        font-size: 0.65rem;
    }
    
    .video-actions {
        justify-content: center;
        flex-wrap: wrap;
    }
    
    .video-actions .btn {
        flex: 1;
        min-width: 100px;
        padding: 0.5rem 0.75rem;
        font-size: 0.8rem;
    }
    
    /* Mobile size summary */
    .mobile-size-summary {
        display: flex;
        flex-wrap: wrap;
        gap: 0.5rem;
        justify-content: center;
        padding: 0.5rem 0;
        border-top: 1px solid var(--border-color);
        margin-top: 0.5rem;
    }
    
    .mobile-size-summary .size-item {
        padding: 0.3rem 0.6rem;
        border-radius: 6px;
        font-size: 0.75rem;
        font-weight: 500;
    }
    
    .mobile-size-summary .size-orig {
        color: var(--text-secondary);
        background: rgba(139, 148, 158, 0.15);
    }
    
    .mobile-size-summary .size-new {
        color: var(--accent-purple);
        background: rgba(163, 113, 247, 0.15);
    }
    
    .mobile-size-summary .size-savings {
        font-weight: 600;
    }
    
    .mobile-size-summary .size-savings.size-reduced {
        color: #4caf50;
        background: rgba(76, 175, 80, 0.2);
    }
    
    .mobile-size-summary .size-savings.size-increased {
        color: #ff9800;
        background: rgba(255, 152, 0, 0.2);
    }
    
    /* Summary bar mobile */
    .video-summary {
        flex-wrap: wrap;
        gap: 0.5rem;
        padding: 0.5rem;
        font-size: 0.8rem;
    }
    
    .video-summary span {
        padding: 0.2rem 0.5rem;
    }
    
    /* Footer mobile */
    .footer {
        flex-direction: column;
        align-items: center;
        gap: 0.25rem;
        padding: 0.5rem 1rem;
        font-size: 0.7rem;
    }
}

/* Extra small screens (< 400px) */
@media (max-width: 400px) {
    .card-metrics {
        flex-wrap: wrap;
    }
    
    .metric {
        min-width: 80px;
    }
    
    .card-sizes {
        font-size: 0.85rem;
    }
    
    .video-actions .btn span {
        display: none;
    }
}
'''
