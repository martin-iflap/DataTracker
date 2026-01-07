from typing import Tuple
import os


def initialize_tracker() -> Tuple[bool, str]:
    """Initialize the .data_tracker directory and config.json file
    Returns: Tuple[bool, str]: (success, message)
    """
    tracker_path = os.path.join(os.getcwd(), ".data_tracker")
    if os.path.exists(tracker_path):
        return False, "Data tracker already initialized"
    try:
        os.makedirs(os.path.join(tracker_path, "data"))
        # config_path = os.path.join(tracker_path, "config.json")
        # with open(config_path, "w") as f:
        #     f.write("{}")
        return True, "Data tracker initialized successfully"
    except OSError as e:
        raise Exception(f"Failed to initialize data tracker: {e}")

