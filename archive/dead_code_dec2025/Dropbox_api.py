import dropbox
from config import DROPBOX_ACCESS_TOKEN


def test_dropbox_connection():
    """Test Dropbox API connection and list some files."""
    try:
        # Initialize client
        dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)
        
        # Test connection
        account = dbx.users_get_current_account()
        print(f"✅ Connected successfully!")
        print(f"Account: {account.name.display_name}")
        print(f"Email: {account.email}")
        
        # List files in root
        print(f"\n📁 Files in root folder:")
        result = dbx.files_list_folder("")
        
        for entry in result.entries[:5]:  # Show first 5 files
            if isinstance(entry, dropbox.files.FileMetadata):
                size_mb = entry.size / (1024 * 1024)
                print(f"  📄 {entry.name} ({size_mb:.2f} MB)")
            elif isinstance(entry, dropbox.files.FolderMetadata):
                print(f"  📁 {entry.name}/")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def list_files_in_folder(folder_path: str):
    """List files in a specific Dropbox folder."""
    try:
        # Initialize client
        dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)
        
        print(f"\n📁 Files in folder: {folder_path}")
        print("=" * 50)
        
        # List files in the specified folder
        result = dbx.files_list_folder(folder_path)
        
        if not result.entries:
            print("  (No files found)")
            return
        
        file_count = 0
        folder_count = 0
        
        for entry in result.entries:
            if isinstance(entry, dropbox.files.FileMetadata):
                file_count += 1
                size_mb = entry.size / (1024 * 1024)
                print(f"  📄 {entry.name} ({size_mb:.2f} MB)")
                print(f"     Modified: {entry.server_modified}")
                if entry.content_hash:
                    print(f"     Content Hash: {entry.content_hash[:16]}...")
                print()
                
            elif isinstance(entry, dropbox.files.FolderMetadata):
                folder_count += 1
                print(f"  📁 {entry.name}/")
                print(f"     ID: {entry.id}")
                print()
        
        # Handle pagination if there are more files
        while result.has_more:
            result = dbx.files_list_folder_continue(result.cursor)
            for entry in result.entries:
                if isinstance(entry, dropbox.files.FileMetadata):
                    file_count += 1
                    size_mb = entry.size / (1024 * 1024)
                    print(f"  📄 {entry.name} ({size_mb:.2f} MB)")
                elif isinstance(entry, dropbox.files.FolderMetadata):
                    folder_count += 1
                    print(f"  📁 {entry.name}/")
        
        print(f"\nSummary: {file_count} files, {folder_count} folders")
        
    except dropbox.exceptions.ApiError as e:
        if e.error.is_path() and e.error.get_path().is_not_found():
            print(f"❌ Folder not found: {folder_path}")
            print("💡 Available folders at root level:")
            # List root to show available paths
            try:
                root_result = dbx.files_list_folder("")
                for entry in root_result.entries:
                    if isinstance(entry, dropbox.files.FolderMetadata):
                        print(f"  📁 {entry.name}")
            except:
                pass
        else:
            print(f"❌ API Error: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")


def search_files(query: str, path: str = ""):
    """Search for files in Dropbox by name."""
    try:
        dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)
        
        print(f"\n🔍 Searching for '{query}' in {path if path else 'entire Dropbox'}...")
        print("=" * 50)
        
        # Search for files
        search_result = dbx.files_search_v2(query=query, options=dropbox.files.SearchOptions(path=path))
        
        if not search_result.matches:
            print("  No files found matching your search.")
            return
        
        for match in search_result.matches[:10]:  # Show first 10 results
            metadata = match.metadata.get_metadata()
            if isinstance(metadata, dropbox.files.FileMetadata):
                size_mb = metadata.size / (1024 * 1024)
                print(f"  📄 {metadata.name}")
                print(f"     Path: {metadata.path_display}")
                print(f"     Size: {size_mb:.2f} MB")
                print(f"     Modified: {metadata.server_modified}")
                print()
        
    except Exception as e:
        print(f"❌ Search error: {e}")


if __name__ == "__main__":
    # Test basic connection
    if test_dropbox_connection():
        
        # Try to access the specific folder path
        # Based on the breadcrumb: "All files / Jeff Muscarella / NPI Data Ownership / 2024 Deal Docs"
        folder_paths_to_try = [
            "/Jeff Muscarella/NPI Data Ownership/2024 Deal Docs",
            "/NPI Data Ownership/2024 Deal Docs", 
            "/2024 Deal Docs"
        ]
        
        success = False
        for folder_path in folder_paths_to_try:
            print(f"\n🔍 Trying path: {folder_path}")
            try:
                dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)
                result = dbx.files_list_folder(folder_path)
                # If we get here, the path exists
                list_files_in_folder(folder_path)
                success = True
                break
            except dropbox.exceptions.ApiError as e:
                if e.error.is_path() and e.error.get_path().is_not_found():
                    print(f"❌ Path not found: {folder_path}")
                    continue
                else:
                    print(f"❌ Other error: {e}")
                    break
            except Exception as e:
                print(f"❌ Error: {e}")
                break
        
        if not success:
            print("\n🔍 Let's explore the folder structure step by step:")
            print("\n📁 Root level folders:")
            try:
                dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)
                result = dbx.files_list_folder("")
                for entry in result.entries:
                    if isinstance(entry, dropbox.files.FolderMetadata):
                        print(f"  📁 {entry.name}")
            except Exception as e:
                print(f"❌ Error listing root: {e}")
        
        # You can also search for specific files
        print("\n" + "="*60)
        search_files("2024", "")  # Search for files containing "2024"