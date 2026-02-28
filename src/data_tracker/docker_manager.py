from typing import Tuple
import subprocess
import os

def is_docker_installed() -> bool:
    """Check if Docker is installed on the system.
     - Run 'docker --version' command and check output
    """
    try:
        result = subprocess.run(['docker', '--version'], capture_output=True, text=True, check=True)
        return "Docker version" in result.stdout
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False

def transform_data(image: str, input_data: str, output_data: str,
                   command: str, force: bool) -> Tuple[bool, str]:
    """Run a docker container to transform data.
     - image: Docker image to use
     - input_data: Path to input data directory
     - output_data: Path to output data directory
     - command: Command to run inside the container,
                it must read from /input and write to /output
    """
    try:
        input_abs = os.path.abspath(input_data)
        output_abs = os.path.abspath(output_data)
        if not os.path.exists(input_abs):
            return False, f"Input data path does not exist: {input_abs}"

        os.makedirs(output_abs, exist_ok=True)

        if not force:
            if '/input' not in command or '/output' not in command:
                return False, (
                    "Command validation failed: Missing required mount paths.\n\n"
                    "Your command must reference the mounted directories:\n"
                    "  • /input  - for reading input data\n"
                    "  • /output - for writing transformed results\n\n"
                    f"Your command: {command}\n\n"
                    "Use --force to bypass this check if your Docker image\n"
                    "handles paths differently (advanced usage)."
                )

        docker_command = [
            'docker', 'run', '--rm',
            '-v', f'{input_abs}:/input:ro',
            '-v', f'{output_abs}:/output',
            image,
            '/bin/sh', '-c', command
        ]
        result = subprocess.run(docker_command, capture_output=True, text=True)
        if result.returncode != 0:
            return False, f"Data transformation failed:\n{result.stderr}"

        if not os.listdir(output_abs):
            return False, (
                "Transformation completed but output directory is empty.\n"
                "Your command may not have written to /output correctly.\n"
                f"stdout: {result.stdout}\n"
                f"stderr: {result.stderr}"
            )
        message = f"Data transformation completed successfully."
        if result.stdout:
            message += f"\nOutput: {result.stdout}"
        return True, message
    except Exception as e:
        return False, f"An error occurred while running the Docker container: {e}"