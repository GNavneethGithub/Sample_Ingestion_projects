import re

def is_valid_header_line(line: str) -> bool:
    # Replace this with actual logic if you have better criteria
    return "GROUP_NAME" in line and "GROUP_MEMBER" in line

def parse_single_farm_data(file_path, farm_name, timestamp_str):
    """Parse a single farm's data and return a list of user share records."""

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
                            break  # Stop after processing the first valid block
                        else:
                            inside_block = False
                            continue

                    # Skip empty or comment lines
                    if not stripped_line or stripped_line.startswith('#'):
                        continue

                    # Check for header
                    if is_valid_header_line(stripped_line):
                        found_valid_block = True
                        continue

                    # Clean and extract parts
                    cleaned = stripped_line.split("#")[0].strip()
                    match = re.match(r'^(\S+)\s*\(\s*([^)]+?)\s*\)\s*\(\s*([^)]+?)\s*\)', cleaned)


                    if not match:
                        print(f"[WARN] Could not parse line: {stripped_line}")
                        continue

                    group_name = match.group(1)
                    group_members_raw = match.group(2)
                    user_shares_raw = match.group(3)

                    group_members = group_members_raw.split()
                    share_matches = re.findall(r'\[([^\]]+),\s*(\d+)\]', user_shares_raw)
                    share_dict = {user.strip(): int(fairshare) for user, fairshare in share_matches}
                    default_share = share_dict.get("default", 0)

                    # Determine PO
                    po_match = re.match(r'^(.*)_users', group_name)
                    po = po_match.group(1) if po_match else group_name

                    for user in group_members:
                        fairshare = share_dict.get(user, default_share)

                        record = {
                            "farm": farm_name,
                            "group": group_name,
                            "user_name": user,
                            "fairshare": fairshare,
                            "@timestamp": timestamp_str,
                            "po": po,
                            "key": f"{farm_name}|{po}|{user}"
                        }

                        parsed_records.append(record)

        print(f"[SUMMARY] Parsed total {len(parsed_records)} records from file: {file_path}")
        return parsed_records

    except Exception as e:
        return []
