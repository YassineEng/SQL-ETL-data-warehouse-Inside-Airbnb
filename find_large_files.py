
import subprocess
import sys

def find_large_files():
    """Finds the largest files in the Git repository history."""
    try:
        # Command to get all git objects and their sizes
        command = (
            'git rev-list --objects --all | ' + 
            'git cat-file --batch-check="%(objecttype) %(objectname) %(objectsize) %(rest)"'
        )

        # Execute the command
        # We use shell=True because of the pipe `|`
        result = subprocess.run(command, shell=True, capture_output=True, text=True, check=True)

        lines = result.stdout.strip().split('\n')
        
        blobs = []
        for line in lines:
            if not line.startswith('blob'):
                continue
            
            try:
                parts = line.split()
                size_bytes = int(parts[2])
                path = ' '.join(parts[3:])
                blobs.append((size_bytes, path))
            except (IndexError, ValueError):
                # Ignore lines that can't be parsed
                continue

        # Sort by size in descending order
        blobs.sort(key=lambda x: x[0], reverse=True)

        print("--- Top 20 Largest Files in Repository History ---")
        print("{:<12} | {}".format("Size", "File Path"))
        print("-" * 60)

        for i, (size, path) in enumerate(blobs[:20]):
            size_mb = size / (1024 * 1024)
            print("{:<12.2f}MB | {}".format(size_mb, path))

    except FileNotFoundError:
        print("Error: `git` command not found. Please ensure Git is installed and in your PATH.")
    except subprocess.CalledProcessError as e:
        print(f"Error executing Git command: {e}")
        print(f"Stderr: {e.stderr}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    find_large_files()
