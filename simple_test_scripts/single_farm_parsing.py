import re

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

                    # Skip comments and empty lines
                    if not stripped_line or stripped_line.startswith('#'):
                        continue

                    # Check for valid header
                    if is_valid_header_line(stripped_line):
                        found_valid_block = True
                        continue

                    # Process data lines
                    cleaned = stripped_line.split("#")[0].strip()
                    parts = re.split(r'\s+', cleaned, maxsplit=2)

                    if len(parts) < 3:
                        continue

                    group_name = parts[0]
                    group_members_raw = parts[1].strip().strip("()")
                    user_shares = parts[2]

                    # Extract group members (comma-separated)
                    group_members = [m.strip() for m in group_members_raw.split(",") if m.strip()]

                    # Parse PO (e.g., from "xyz_users" → "xyz")
                    po_match = re.match(r'^(.*)_users', group_name)
                    po = po_match.group(1) if po_match else group_name

                    # Extract user share assignments
                    share_matches = re.findall(r'\[([^\]]+),\s*(\d+)\]', user_shares)
                    share_dict = {user.strip(): int(fairshare) for user, fairshare in share_matches}
                    default_share = share_dict.get("default", 0)

                    # Assign fairshare to each group member
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

        return parsed_records
    except Exception as e:
        raise RuntimeError(f"Failed to parse {file_path}") from e




