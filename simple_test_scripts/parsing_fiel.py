import json
import os
import re
from datetime import datetime
import pytz


def parse_multiple_farms_to_ndjson(    farm_list,    file_path_template,    temp_output_dir="/tmp",    timezone="UTC" ):
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


def count_json_records(    farm_list,    file_path_template,    temp_output_dir="/tmp",    timezone="UTC" ):
    """
    Parse multiple farms to NDJSON and return record count + file path.
    
    Parameters:
    - farm_list: List of farm names
    - file_path_template: Path template with {farm} placeholder
    - temp_output_dir: Directory to save output file
    - timezone: Timezone for timestamps (always converts to UTC/Zulu)
    
    Returns:
    - tuple: (record_count, file_path) or (None, None) if failed
    """
    
    # Call the parsing function
    output_path = parse_multiple_farms_to_ndjson(
        farm_list=farm_list,
        file_path_template=file_path_template,
        temp_output_dir=temp_output_dir,
        timezone=timezone
    )
    
    # If parsing failed, return None values
    if not output_path:
        print("[INFO] No output file created - parsing failed or no data found")
        return None, None
    
    # Count records in the NDJSON file
    try:
        with open(output_path, "r") as f:
            record_count = sum(1 for line in f if line.strip())
        
        print(f"[INFO] Record count: {record_count}")
        print(f"[INFO] File location: {output_path}")
        
        return record_count, output_path
        
    except Exception as e:
        print(f"[ERROR] Failed to count records in file {output_path}: {e}")
        return None, None









