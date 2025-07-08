import logging
import base64
import re
from typing import List
from airflow.providers.ssh.hooks.ssh import SSHHook

logger = logging.getLogger(__name__)

def S2S_count_records(config: dict) -> int:
    """
    Complete Source-to-Stage count pipeline.
    Generates count script and executes it on remote server.
    
    Args:
        config: Dictionary containing all required parameters
    
    Returns:
        Integer count of records, or 0 if failed/no records
    """
    farm_list = config["farm_list"]
    farm_path_template = config["farm_path_template"]
    ssh_conn_id = config["ssh_conn_id"]
    remote_script_path = config["remote_script_path"]
    python_path = config["python_path"]

    logger.info("=== Starting Source-to-Stage Record Count ===")
    
    try:
        # Generate count script
        logger.info("Generating record count script...")
        script_content = generate_count_script(
            farm_list=farm_list,
            farm_path_template=farm_path_template
        )
        
        logger.info("Count script generated successfully")
        
        # Execute on remote server and capture output
        logger.info("Executing script on remote server...")
        encoded_script = base64.b64encode(script_content.encode('utf-8')).decode('ascii')
        
        remote_cmd = f"""
echo '{encoded_script}' | base64 -d > {remote_script_path} && \
{python_path} {remote_script_path} ; \
rm -f {remote_script_path}
"""
        
        ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
        result = ssh_hook.run_ssh_command(remote_cmd)
        
        if not result:
            logger.error("No output received from remote execution")
            return 0
        
        logger.info(f"Remote execution output: {result}")
        
        # Extract count from output
        count = extract_count_from_output(result)
        
        logger.info(f"=== Record Count Completed: {count} records ===")
        return count
        
    except Exception as e:
        logger.error(f"Record count process failed: {e}")
        return 0

def extract_count_from_output(output: str) -> int:
    """
    Extract the final count from the remote script output.
    Looks for the last line that contains "FINAL_COUNT: <number>"
    
    Args:
        output: Raw output from SSH command execution
    
    Returns:
        Extracted count or 0 if not found
    """
    try:
        # Look for lines containing "FINAL_COUNT:"
        lines = output.strip().split('\n')
        
        for line in reversed(lines):  # Start from the end
            if "FINAL_COUNT:" in line:
                # Extract number after "FINAL_COUNT:"
                match = re.search(r'FINAL_COUNT:\s*(\d+)', line)
                if match:
                    count = int(match.group(1))
                    logger.info(f"Successfully extracted count: {count}")
                    return count
        
        logger.warning("No FINAL_COUNT found in output, trying to parse last numeric line")
        
        # Fallback: look for last line that's just a number
        for line in reversed(lines):
            line = line.strip()
            if line.isdigit():
                count = int(line)
                logger.info(f"Extracted count from numeric line: {count}")
                return count
        
        logger.error("Could not extract count from output")
        return 0
        
    except Exception as e:
        logger.error(f"Failed to extract count from output: {e}")
        return 0

def generate_count_script(farm_list, farm_path_template):
    """
    Generate the remote counting script that outputs the final count.
    """
    # Use the same static script part but modify the main function
    static_script_part = r'''
import os
import re
import json
import time
import logging
from typing import List, Dict, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def extract_valid_usergroup_block(lines: List[str]) -> Tuple[List[str], int]:
    """
    Extract the first UserGroup block that doesn't contain PRIORITY fields.
    Ignores commented lines (starting with #) when checking for PRIORITY.
    
    Args:
        lines: All lines from the lsb.users file
    
    Returns:
        Tuple of (block_lines, block_start_line_number)
    """
    current_block = []
    in_block = False
    block_start_line = 0
    
    for line_num, line in enumerate(lines, 1):
        stripped_line = line.strip()
        
        # Skip empty lines
        if not stripped_line:
            continue
            
        if "Begin UserGroup" in stripped_line:
            in_block = True
            current_block = []
            block_start_line = line_num
            continue
            
        if "End UserGroup" in stripped_line and in_block:
            in_block = False
            
            if not current_block:
                continue
                
            # Only check non-commented lines for PRIORITY
            active_lines = [line for line in current_block if not line.startswith('#')]
            active_text = ' '.join(active_lines)
            
            # Check for PRIORITY in active lines only
            if "PRIORITY" in active_text:
                logger.info("Skipping UserGroup block at line %d: contains PRIORITY", block_start_line)
                current_block = []
                continue
                
            # Check if block has actual share data in active lines
            has_share_data = '[' in active_text and ']' in active_text
            
            if not has_share_data:
                logger.info("Skipping UserGroup block at line %d: no share data found", block_start_line)
                current_block = []
                continue
                
            # Found valid block with share data
            logger.info("Found valid UserGroup block at line %d", block_start_line)
            return current_block, block_start_line
            
        if in_block:
            current_block.append(stripped_line)
    
    return [], 0

def parse_usergroup_block(block_lines: List[str], farm_name: str, ts_str: str) -> List[Dict]:
    """
    Parse a valid UserGroup block to extract user records.
    Each group is on a single line with format: group_name (members) [shares_data]
    
    Args:
        block_lines: Lines from a valid UserGroup block
        farm_name: Name of the farm being processed
        ts_str: Timestamp string for all records
    
    Returns:
        List of user record dictionaries
    """
    parsed_records = []
    
    for line in block_lines:
        # Skip empty lines and commented lines
        if not line.strip() or line.strip().startswith('#'):
            continue
            
        # Remove inline comments but keep the data part
        line_clean = line.split('#')[0].strip()
        if not line_clean:
            continue
            
        # Skip header lines
        if any(header in line_clean for header in ["GROUP_NAME", "GROUP_MEMBER", "USER_SHARES"]):
            continue
            
        try:
            logger.info("Parsing line: %s", line_clean[:100] + "..." if len(line_clean) > 100 else line_clean)
            
            # Extract group name (first word)
            parts = line_clean.split()
            if not parts:
                continue
                
            group_name = parts[0]
            
            # Check if this line has both members and shares
            if '(' not in line_clean or '[' not in line_clean:
                logger.warning("Line missing members or shares: %s", group_name)
                continue
            
            # Split at first '[' to separate members from shares
            members_part = line_clean.split('[')[0]
            shares_part = '[' + line_clean.split('[', 1)[1]
            
            # Extract members from parentheses in members_part only
            member_matches = re.findall(r'\(([^)]+)\)', members_part)
            if not member_matches:
                logger.warning("No members found in group: %s", group_name)
                continue
                
            # Parse all members (space-separated within parentheses)
            all_members = []
            for member_group in member_matches:
                # Split by whitespace and filter empty strings
                members = [m.strip() for m in member_group.split() if m.strip()]
                all_members.extend(members)
            
            if not all_members:
                logger.warning("No valid members parsed for group: %s", group_name)
                continue
            
            logger.info("Found %d members in group %s", len(all_members), group_name)
            
            # Extract shares from square brackets in shares_part
            share_matches = re.findall(r'\[([^\]]+)\]', shares_part)
            if not share_matches:
                logger.warning("No shares found for group: %s", group_name)
                continue
            
            # Parse shares into a dictionary
            share_dict = {}
            for share_group in share_matches:
                if ',' in share_group:
                    try:
                        user, value = share_group.split(',', 1)
                        user = user.strip()
                        value = int(value.strip())
                        share_dict[user] = value
                    except ValueError as e:
                        logger.warning("Failed to parse share '%s': %s", share_group, str(e))
                        continue
            
            if not share_dict:
                logger.warning("No valid shares parsed for group: %s", group_name)
                continue
            
            logger.info("Found %d share definitions for group %s", len(share_dict), group_name)
            
            # Generate PO name (remove _users suffix if present)
            po = group_name.replace('_users', '') if group_name.endswith('_users') else group_name
            
            # Create records for each member
            records_created = 0
            for member in all_members:
                # Look for fairshare value: exact match -> default -> others
                fairshare = None
                
                if member in share_dict:
                    fairshare = share_dict[member]
                elif 'default' in share_dict:
                    fairshare = share_dict['default']
                elif 'others' in share_dict:
                    fairshare = share_dict['others']
                
                if fairshare is not None:
                    record = {
                        "farm": farm_name,
                        "group": group_name,
                        "user_name": member,
                        "fairshare": fairshare,
                        "timestamp": ts_str,
                        "po": po,
                        "key": "%s|%s|%s" % (farm_name, po, member)
                    }
                    parsed_records.append(record)
                    records_created += 1
                else:
                    logger.debug("No fairshare found for user '%s' in group '%s'", member, group_name)
            
            if records_created > 0:
                logger.info("Created %d records from group '%s'", records_created, group_name)
                
        except Exception as e:
            logger.error("Failed to parse line '%s': %s", line_clean[:100], str(e))
            continue
    
    return parsed_records

def count_ndjson_records(farm_list: List[str], farm_path_template: str) -> int:
    """
    Count the total number of records that would be generated from farm files.
    
    Args:
        farm_list: List of farm names to process
        farm_path_template: Path template with {farm} placeholder
    
    Returns:
        Total number of records found across all farms
    """
    logger.info("Starting record count process for %d farms", len(farm_list))
    
    # Generate timestamp once for all records
    epoch_time = int(time.time())
    ts_str = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(epoch_time))
    
    total_records = 0
    farms_processed = 0
    
    for farm_name in farm_list:
        logger.info("Processing farm: %s", farm_name)
        
        # Build file path
        file_path = farm_path_template.replace('{farm}', farm_name)
        
        # Check if file exists
        if not os.path.exists(file_path):
            logger.warning("Farm '%s' skipped: file not found at %s", farm_name, file_path)
            continue
            
        # Read file
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
        except Exception as e:
            logger.error("Farm '%s' skipped: failed to read file %s: %s", farm_name, file_path, str(e))
            continue
        
        # Extract valid UserGroup block
        block_lines, block_start_line = extract_valid_usergroup_block(lines)
        
        if not block_lines:
            logger.warning("Farm '%s' skipped: no valid UserGroup block found in %s", farm_name, file_path)
            continue
        
        # Parse the block into records
        parsed_records = parse_usergroup_block(block_lines, farm_name, ts_str)
        
        if not parsed_records:
            logger.warning("Farm '%s' skipped: no parseable user records found in %s", farm_name, file_path)
            continue
        
        # Add to total count
        farm_record_count = len(parsed_records)
        logger.info("Farm '%s' contributed %d user records", farm_name, farm_record_count)
        total_records += farm_record_count
        farms_processed += 1
    
    logger.info("Processing complete: %d total records from %d farms", total_records, farms_processed)
    
    if farms_processed == 0:
        logger.warning("No farm files processed successfully")
    
    return total_records
'''

    # Main execution block that outputs the count
    main_block = '''
if __name__ == "__main__":
    farm_list = {farm_list}
    farm_path_template = "{farm_path_template}"

    try:
        count = count_ndjson_records(
            farm_list=farm_list,
            farm_path_template=farm_path_template
        )
        
        # Log for debugging/audit trail
        logger.info("Count process completed successfully with %d records", count)
        
        # Print for SSH capture
        print("FINAL_COUNT: {{}}".format(count))
        
    except Exception as e:
        # Log the error
        logger.error("Count process failed: {{}}".format(e))
        
        # Print failure state for SSH capture
        print("FINAL_COUNT: 0")
        print("ERROR: {{}}".format(e))
        raise
'''.format(
        farm_list=repr(farm_list),
        farm_path_template=farm_path_template
    )
    
    return static_script_part + '\n' + main_block


