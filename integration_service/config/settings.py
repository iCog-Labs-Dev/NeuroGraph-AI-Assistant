"""Configuration management for Integration Service."""  
import os  
from dotenv import load_dotenv  
  
load_dotenv()  
  
class Settings:  
    """Integration Service settings"""  
      
    def __init__(self):  
        # Service URLs  
        self.atomspace_url = os.getenv('ATOMSPACE_API_URL', 'http://atomspace-api:8000')  
        self.miner_url = os.getenv('NEURAL_MINER_URL', 'http://neural-miner:5000')  
          
        # Timeouts 
        self.atomspace_timeout = int(os.getenv('ATOMSPACE_TIMEOUT', '600'))  
        self.miner_timeout = int(os.getenv('MINER_TIMEOUT', '1800'))  
          
        # CSV caching  
        self.csv_cache_dir = os.getenv('CSV_CACHE_DIR', './cache')  
          
        # Shared volume  
        self.shared_volume_path = os.getenv('SHARED_VOLUME_PATH', '/shared/output')  
  
settings = Settings()