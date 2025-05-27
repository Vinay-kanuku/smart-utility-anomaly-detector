from yaml import safe_load
from constants import APP_CONFIG_YAML

def load_yaml(file_path: str) -> dict:
    try:
        with open(file_path, "r") as f:
            return safe_load(f)
    except Exception as e:
        raise e 
    

if __name__ == "__main__":
    ans = load_yaml(APP_CONFIG_YAML)
    print(ans)
    
