#!/usr/bin/env python3
"""
Pipeline Management Utility

Quick utility to check status and manage your document processing pipeline.
"""

import sys
import os
from pathlib import Path

# Add src to path
sys.path.insert(0, 'src')

def check_status():
    """Check current pipeline status"""
    print("📊 Checking pipeline status...")
    os.system("python enhanced_production_pipeline.py --status")

def resume_processing():
    """Resume processing from where it left off"""
    print("🔄 Resuming processing...")
    os.system("python enhanced_production_pipeline.py --resume")

def discovery_only():
    """Run discovery only to find all files"""
    print("🔍 Running discovery only...")
    os.system("python enhanced_production_pipeline.py --discovery-only")

def show_cache_info():
    """Show information about cache files"""
    print("💾 Cache Information:")
    
    # Check discovery cache
    discovery_cache = Path("cache/discovery")
    if discovery_cache.exists():
        cache_files = list(discovery_cache.glob("*.json"))
        print(f"🔍 Discovery cache: {len(cache_files)} files")
        for file in cache_files:
            size_mb = file.stat().st_size / (1024 * 1024)
            print(f"   📄 {file.name}: {size_mb:.1f}MB")
    
    # Check batch cache
    batch_cache = Path("cache/batches")
    if batch_cache.exists():
        batch_files = list(batch_cache.glob("*.json"))
        print(f"📦 Batch cache: {len(batch_files)} files")
        
        # Show batch state file
        state_files = [f for f in batch_files if f.name.startswith("batch_state_")]
        if state_files:
            for file in state_files:
                size_mb = file.stat().st_size / (1024 * 1024)
                print(f"   📊 {file.name}: {size_mb:.1f}MB")
    
    print()

def show_menu():
    """Show main menu"""
    print("🚀 Document Processing Pipeline Manager")
    print("=" * 50)
    print("1. Check Status")
    print("2. Resume Processing") 
    print("3. Discovery Only")
    print("4. Show Cache Info")
    print("5. Fresh Start (Warning: Clears all progress)")
    print("6. Test with 100 docs")
    print("0. Exit")
    print()

def main():
    """Main menu loop"""
    
    while True:
        show_menu()
        
        try:
            choice = input("Select option (0-6): ").strip()
            print()
            
            if choice == "0":
                print("👋 Goodbye!")
                break
                
            elif choice == "1":
                check_status()
                
            elif choice == "2":
                resume_processing()
                
            elif choice == "3":
                discovery_only()
                
            elif choice == "4":
                show_cache_info()
                
            elif choice == "5":
                print("⚠️ WARNING: This will delete all progress!")
                confirm = input("Type 'DELETE' to confirm: ")
                if confirm == "DELETE":
                    os.system("python enhanced_production_pipeline.py --fresh")
                else:
                    print("❌ Cancelled")
                    
            elif choice == "6":
                print("🧪 Testing with 100 documents...")
                os.system("python enhanced_production_pipeline.py --max-docs 100 --resume")
                
            else:
                print("❌ Invalid choice")
                
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            break
            
        except Exception as e:
            print(f"❌ Error: {e}")
            
        print("\n" + "="*50 + "\n")

if __name__ == "__main__":
    main() 