import json
import os
import re
from datetime import datetime
import pytz


def parse_multiple_farms_to_ndjson(
    farm_list,
    file_path_template,
    temp_output_dir="/tmp",
    timezone="UTC"
):
    """
    Parse multiple farms and save all data to single .json file in NDJSON format.
    Always uses Zulu time format regardless of input timezone.
    
    Returns:
    - output_file_path: Full path to the created .json file, or None if no data found
    """
    
    # Generate timestamp - always convert to UTC and use Zulu format
    try:
        tz = pytz.timezone(timezone)
        current_dt = datetime.now(tz)
        # Convert to UTC and format as Zulu time
        current_utc = current_dt.utctimetuple()
        current_timestamp = datetime(*current_utc[:6]).strftime("%Y-%m-%dT%H:%M:%SZ")
        timestamp_file = datetime(*current_utc[:6]).strftime("%Y%m%d_%H%M%S")
        
    except:
        # Fallback to UTC
        current_dt = datetime.now(pytz.UTC)
        current_timestamp = current_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        timestamp_file = current_dt.strftime("%Y%m%d_%H%M%S")
    
    output_filename = f"all_farms_fairshare_{timestamp_file}.json"
    output_path = os.path.join(temp_output_dir, output_filename)
    
    all_records = []
    
    for farm in farm_list:
        file_path = file_path_template.replace("{farm}", farm)
        
        if not os.path.isfile(file_path):
            print(f"[INFO] Farm {farm}: File not available - {file_path}")
            continue
        
        farm_records = parse_single_farm_data(file_path, farm, current_timestamp)
        
        if farm_records:
            all_records.extend(farm_records)
            print(f"[INFO] Farm {farm}: {len(farm_records)} records processed")
        else:
            print(f"[INFO] Farm {farm}: UserGroup block not available or no data found")
    
    if not all_records:
        print(f"[INFO] No data found across all farms. No file created.")
        return None
    
    try:
        os.makedirs(temp_output_dir, exist_ok=True)
        
        with open(output_path, "w") as f:
            for record in all_records:
                json_line = json.dumps(record, separators=(',', ':'))
                f.write(json_line + '\n')
        
        print(f"[INFO] Created file: {output_path} with {len(all_records)} total records")
        return output_path
        
    except Exception as e:
        print(f"[INFO] Failed to create output file: {e}")
        return None

def parse_single_farm_data(file_path, farm_name, timestamp_str):
    """Parse a single farm's data and return records list. Returns empty list if no valid block found."""
    
    inside_block = False
    found_valid_block = False
    parsed_records = []
    
    try:
        with open(file_path, "r") as f:
            for line in f:
                stripped_line = line.rstrip("\n")
                
                if "Begin UserGroup" in stripped_line:
                    inside_block = True
                    continue
                
                if inside_block:
                    if "End UserGroup" in stripped_line:
                        if found_valid_block:
                            break  # Exit after processing valid block
                        else:
                            inside_block = False  # Reset and look for next block
                            continue
                    
                    # Check for valid header
                    if is_valid_header_line(stripped_line):
                        found_valid_block = True
                        continue
                    
                    # Process data lines
                    if (found_valid_block and 
                        stripped_line and 
                        not stripped_line.startswith('#')):
                        
                        cleaned = stripped_line.split("#")[0].strip()
                        parts = re.split(r'\s+', cleaned, maxsplit=2)
                        
                        if len(parts) < 3:
                            continue
                        
                        group_name = parts[0]
                        user_shares = parts[2]
                        
                        po_match = re.match(r'^(.+?)_users', group_name)
                        po = po_match.group(1) if po_match else group_name
                        
                        share_matches = re.findall(r'\[([^,]+),\s*(\d+)\]', user_shares)
                        
                        for user, fairshare in share_matches:
                            user = user.strip()
                            fairshare = int(fairshare)
                            
                            record = {
                                "farm": farm_name,
                                "group": group_name,
                                "user_name": user,
                                "fairshare": fairshare,
                                "timestamp": timestamp_str,
                                "po": po,
                                "key": f"{farm_name}|{po}|{user}"
                            }
                            
                            parsed_records.append(record)
        
        return parsed_records
        
    except:
        return []

def is_valid_header_line(line):
    """Check if line contains required column headers."""
    
    cleaned_line = line.split('#')[0].strip()
    columns = re.split(r'\s+', cleaned_line)
    
    if len(columns) < 3:
        return False
    
    expected = ["GROUP_NAME", "GROUP_MEMBER", "USER_SHARES"]
    
    for i in range(3):
        if columns[i] != expected[i]:
            return False
    
    return True

# # Usage:
# farms = ["farm1", "farm2", "farm3", "farm4"]
# template = "/data/{farm}/config/lsb.users"

# output_path = parse_multiple_farms_to_ndjson(
#     farm_list=farms,
#     file_path_template=template,
#     temp_output_dir="/tmp"
# )

# if output_path:
#     print(f"Success: {output_path}")
# else:
#     print("No output file created")




import os
import json

def create_test_environment():
    """
    Create sample lsb.users files matching production structure:
    /global/lsf/cells/{farm}/conf/lsbatch/{farm}/configdir/lsb.users
    """
    
    # Farm list from your config
    farms = ["amscae_lsf_grid", "pythia", "us01_swe", "us01_vcprod", 
             "us01_vcstrain", "us01_vctrain", "edag_batch1", 
             "edag_batch2", "edag_int", "edag_perf", "edag_shared"]
    
    # Base directory for testing
    base_dir = "/tmp/test_lsf"
    
    # Template path
    template = "/global/lsf/cells/{farm}/conf/lsbatch/{farm}/configdir/lsb.users"
    
    print("Creating test environment...")
    
    # Sample UserGroup configurations
    sample_configs = {
        "amscae_lsf_grid": """# Sample lsb.users for amscae_lsf_grid
Begin UserGroup
GROUP_NAME       GROUP_MEMBER                                                  USER_SHARES                             #GROUP_ADMIN
cme_users        (kmli mpiyush anmolm subdas skwon majoshi mvp)                ([user5, 16] [user6, 34] [user7, 15])   #()
admin_users      (lsfadmin ikeoka)                                             ([default, 10])                        #()
batch_users      (admin_users di_users cme_users pov_users)                   ()                                      #()
End UserGroup""",
        
        "pythia": """# Sample lsb.users for pythia
Begin UserGroup
GROUP_NAME       GROUP_MEMBER                                                  USER_SHARES
ais_users        (zhengxu xihui giuliog trust cathal esen chanvan)             ([cpuff, 200] [zhengxu, 14] [xihui, 14] [trust, 14])
ml_users         (researcher1 researcher2 datascientist1)                      ([researcher1, 50] [default, 25])
End UserGroup""",
        
        "us01_swe": """# Sample lsb.users for us01_swe
Begin UserGroup  
GROUP_NAME       GROUP_MEMBER                     USER_SHARES                    #GROUP_ADMIN
dev_users        (developer1 developer2 tester1)  ([developer1, 40] [tester1, 20])  #()
qa_users         (qa_lead qa_analyst)             ([qa_lead, 30] [default, 10])      #()
End UserGroup""",
        
        "us01_vcprod": """# Sample lsb.users for us01_vcprod
Begin UserGroup
GROUP_NAME       GROUP_MEMBER                     USER_SHARES
prod_users       (produser1 produser2 sysadmin)   ([produser1, 100] [sysadmin, 50])
End UserGroup""",
        
        "edag_batch1": """# Sample lsb.users for edag_batch1 - NO UserGroup block
Begin User
USER_NAME     MAX_PEND_JOBS     MAX_JOBS
user1         800               1000
default       1000              1000
End User""",
        
        "edag_int": """# Sample lsb.users for edag_int - Empty UserGroup
Begin UserGroup
GROUP_NAME       GROUP_MEMBER     USER_SHARES
End UserGroup"""
    }
    
    # Create test files
    created_files = []
    
    for farm in farms:
        # Generate file path
        file_path = template.replace("{farm}", farm)
        # Replace /global with /tmp/test_lsf for testing
        test_file_path = file_path.replace("/global", base_dir)
        
        # Create directory structure
        os.makedirs(os.path.dirname(test_file_path), exist_ok=True)
        
        # Create file content
        if farm in sample_configs:
            content = sample_configs[farm]
        elif farm in ["us01_vcstrain", "us01_vctrain"]:
            # These farms will have missing files (test case)
            print(f"Skipping {farm} - testing missing file scenario")
            continue
        else:
            # Default content for remaining farms
            content = f"""# Sample lsb.users for {farm}
Begin UserGroup
GROUP_NAME       GROUP_MEMBER          USER_SHARES
default_users    (user1 user2 user3)   ([user1, 25] [user2, 25] [default, 50])
End UserGroup"""
        
        # Write file
        with open(test_file_path, "w") as f:
            f.write(content)
        
        created_files.append(test_file_path)
        print(f"Created: {test_file_path}")
    
    return base_dir, template.replace("/global", base_dir), farms

def test_parsing_function():
    """Test the parsing function with sample data."""
    
    # Create test environment
    base_dir, test_template, farms = create_test_environment()
    
    print(f"\n{'='*80}")
    print("TESTING PARSING FUNCTION")
    print(f"{'='*80}")
    
    # Test the parsing function
    output_path = parse_multiple_farms_to_ndjson(
        farm_list=farms,
        file_path_template=test_template,
        temp_output_dir="/tmp",
        timezone="UTC"
    )
    
    if output_path:
        print(f"\n{'='*80}")
        print("PARSING RESULTS")
        print(f"{'='*80}")
        print(f"Output file: {output_path}")
        
        # Show sample of created NDJSON file
        print(f"\nSample content (first 5 lines):")
        with open(output_path, "r") as f:
            for i, line in enumerate(f):
                if i >= 5:
                    break
                record = json.loads(line.strip())
                print(f"Line {i+1}: farm={record['farm']}, group={record['group']}, user={record['user_name']}, fairshare={record['fairshare']}")
        
        # Count total records
        with open(output_path, "r") as f:
            total_lines = sum(1 for line in f)
        print(f"\nTotal records in file: {total_lines}")
        
    else:
        print("No output file created - check for errors above")
    
    # Cleanup
    print(f"\n{'='*80}")
    print("CLEANUP")
    print(f"{'='*80}")
    import shutil
    if os.path.exists(base_dir):
        shutil.rmtree(base_dir)
        print(f"Cleaned up test directory: {base_dir}")



def test_parsing_function_keep_file():
    """Test the parsing function and keep the output file."""
    
    # Create test environment
    base_dir, test_template, farms = create_test_environment()
    
    print(f"\n{'='*80}")
    print("TESTING PARSING FUNCTION")
    print(f"{'='*80}")
    
    # Test the parsing function
    output_path = parse_multiple_farms_to_ndjson(
        farm_list=farms,
        file_path_template=test_template,
        temp_output_dir="/tmp",
        timezone="UTC"
    )
    
    if output_path:
        print(f"\n{'='*80}")
        print("PARSING RESULTS")
        print(f"{'='*80}")
        print(f"Output file: {output_path}")
        
        # Show actual file contents
        print(f"\nFull file contents:")
        with open(output_path, "r") as f:
            for i, line in enumerate(f, 1):
                print(f"Line {i}: {line.strip()}")
        
        print(f"\n*** FILE PRESERVED AT: {output_path} ***")
        
    # Clean up test directories but KEEP the output file
    import shutil
    if os.path.exists(base_dir):
        shutil.rmtree(base_dir)
        print(f"Cleaned up test directory: {base_dir}")
        print(f"Output file still available at: {output_path}")





# Run the test
if __name__ == "__main__":
    # Make sure we have the parsing function available
    # (Include the parse_multiple_farms_to_ndjson function we created earlier)
    
    test_parsing_function_keep_file()