#!/usr/bin/env python3
"""
Pipeline Wrapper - Runs pipeline with monitoring and crash detection

Wraps your production pipeline and sends alerts when:
- Pipeline crashes unexpectedly  
- Pipeline completes successfully
- Pipeline takes longer than expected
- High error rates detected
"""

import subprocess
import time
import signal
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path
import logging

class PipelineWrapper:
    """Wraps pipeline execution with monitoring and alerts"""
    
    def __init__(self, 
                 phone_number: str,
                 pipeline_script: str = "production_pipeline_batch.py",
                 max_runtime_hours: int = 6,
                 error_threshold: int = 10):
        """
        Initialize pipeline wrapper
        
        Args:
            phone_number: Phone number for iMessage alerts
            pipeline_script: Script to run and monitor
            max_runtime_hours: Alert if pipeline runs longer than this
            error_threshold: Alert if more than this many errors
        """
        self.phone_number = phone_number
        self.pipeline_script = pipeline_script
        self.max_runtime = timedelta(hours=max_runtime_hours)
        self.error_threshold = error_threshold
        self.start_time = None
        self.process = None
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - WRAPPER - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
    def send_imessage(self, message: str) -> bool:
        """Send iMessage alert"""
        try:
            script = f'''
            tell application "Messages"
                set targetService to 1st account whose service type = iMessage
                set targetBuddy to buddy "{self.phone_number}" of targetService
                send "{message}" to targetBuddy
            end tell
            '''
            
            result = subprocess.run(
                ['osascript', '-e', script],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            return result.returncode == 0
            
        except Exception as e:
            self.logger.error(f"Error sending alert: {e}")
            return False
    
    def count_errors_in_log(self) -> int:
        """Count errors in the current log file"""
        try:
            error_log = Path("logs/production_errors.log")
            if error_log.exists():
                with open(error_log, 'r') as f:
                    content = f.read()
                    return content.count('ERROR')
        except Exception:
            pass
        return 0
    
    def signal_handler(self, signum, frame):
        """Handle Ctrl+C gracefully"""
        self.logger.info("🛑 Wrapper interrupted by user")
        if self.process:
            self.process.terminate()
            self.send_imessage(f"🛑 Pipeline manually stopped\n⏰ {datetime.now().strftime('%H:%M:%S')}")
        sys.exit(0)
    
    def run_pipeline(self):
        """Run the pipeline with monitoring"""
        
        # Setup signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        
        self.start_time = datetime.now()
        self.logger.info(f"🚀 Starting pipeline: {self.pipeline_script}")
        
        # Send start notification
        self.send_imessage(f"🚀 Pipeline Started\n📄 Script: {self.pipeline_script}\n⏰ {self.start_time.strftime('%H:%M:%S')}")
        
        try:
            # Run the pipeline
            self.process = subprocess.Popen(
                [sys.executable, self.pipeline_script],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            # Monitor the process
            last_error_count = 0
            last_runtime_check = self.start_time
            
            while self.process.poll() is None:
                # Check runtime
                current_time = datetime.now()
                runtime = current_time - self.start_time
                
                # Alert if running too long (check every hour)
                if (current_time - last_runtime_check) > timedelta(hours=1):
                    if runtime > self.max_runtime:
                        message = f"⏰ Pipeline Running Long!\n\n🕐 Runtime: {runtime}\n📊 Still processing...\n⏰ {current_time.strftime('%H:%M:%S')}"
                        self.send_imessage(message)
                    last_runtime_check = current_time
                
                # Check error count (every 10 minutes)
                error_count = self.count_errors_in_log()
                if error_count > last_error_count + self.error_threshold:
                    message = f"🚨 High Error Rate!\n\n❌ Errors: {error_count}\n📈 New errors: {error_count - last_error_count}\n⏰ {current_time.strftime('%H:%M:%S')}"
                    self.send_imessage(message)
                    last_error_count = error_count
                
                time.sleep(60)  # Check every minute
            
            # Pipeline finished
            return_code = self.process.returncode
            end_time = datetime.now()
            runtime = end_time - self.start_time
            final_error_count = self.count_errors_in_log()
            
            # Send completion notification
            if return_code == 0:
                message = f"✅ Pipeline Completed!\n\n⏱️ Runtime: {runtime}\n❌ Errors: {final_error_count}\n⏰ {end_time.strftime('%H:%M:%S')}"
                self.logger.info("✅ Pipeline completed successfully")
            else:
                message = f"💥 Pipeline Failed!\n\n❌ Exit code: {return_code}\n⏱️ Runtime: {runtime}\n🚨 Errors: {final_error_count}\n⏰ {end_time.strftime('%H:%M:%S')}"
                self.logger.error(f"💥 Pipeline failed with exit code: {return_code}")
            
            self.send_imessage(message)
            
            # Show stderr if there were errors
            if return_code != 0:
                stderr_output = self.process.stderr.read()
                if stderr_output:
                    self.logger.error(f"Pipeline stderr: {stderr_output}")
            
            return return_code
            
        except Exception as e:
            error_message = f"💥 Pipeline Wrapper Error!\n\n🐛 Error: {str(e)}\n⏰ {datetime.now().strftime('%H:%M:%S')}"
            self.logger.error(f"Wrapper error: {e}")
            self.send_imessage(error_message)
            return 1


def main():
    """Main function"""
    
    # CONFIGURE THESE SETTINGS
    PHONE_NUMBER = "+1 4044320844"  # Your phone number
    PIPELINE_SCRIPT = "production_pipeline_batch.py"  # Script to monitor
    MAX_RUNTIME_HOURS = 6  # Alert if runs longer than this
    ERROR_THRESHOLD = 10  # Alert if more than this many new errors
    
    print("🤖 Pipeline Wrapper Monitor")
    print("=" * 50)
    print(f"📱 Alerts to: {PHONE_NUMBER}")
    print(f"📄 Monitoring: {PIPELINE_SCRIPT}")
    print(f"⏰ Max runtime: {MAX_RUNTIME_HOURS} hours")
    print(f"🚨 Error threshold: {ERROR_THRESHOLD}")
    print()
    
    if PHONE_NUMBER == "+1234567890":
        print("❌ Please configure your phone number!")
        return
    
    if not Path(PIPELINE_SCRIPT).exists():
        print(f"❌ Pipeline script not found: {PIPELINE_SCRIPT}")
        return
    
    # Create and run wrapper
    wrapper = PipelineWrapper(
        phone_number=PHONE_NUMBER,
        pipeline_script=PIPELINE_SCRIPT,
        max_runtime_hours=MAX_RUNTIME_HOURS,
        error_threshold=ERROR_THRESHOLD
    )
    
    exit_code = wrapper.run_pipeline()
    sys.exit(exit_code)


if __name__ == "__main__":
    main() 