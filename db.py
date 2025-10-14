import os
from supabase import create_client, Client
from typing import Optional, Dict

# 🔥 PROVIDED SUPABASE CREDENTIALS (Hardcoded as requested) 🔥
# Note: Hum 'service_role' key use kar rahe hain
SUPABASE_URL: str = "https://eorilcomhitkpkthfdes.supabase.co"
SUPABASE_KEY: str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVvcmlsY29taGl0a3BrdGhmZGVzIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1Mzk3NTc2MCwiZXhwIjoyMDY5NTUxNzYwfQ.ePTpfwz_qZ3B92JU8wJFxiBWEvQfFfc3yvAxcxYzNfA"

# Aapki table ka naam, jaisa ki aapne diya tha
TABLE_NAME = "Movieshub.org" 

# Global Supabase Client
supabase: Optional[Client] = None

try:
    # Supabase Client ko initialize karte hain
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("Supabase Client Initialized.")
except Exception as e:
    print(f"Error initializing Supabase: {e}")
    supabase = None

def get_movie_data(movie_id_or_uuid: str) -> Optional[Dict]:
    """Supabase se movie data (file_id, title, size) fetch karta hai by ID (UUID)."""
    global supabase
    if not supabase:
        return None
    
    # Hum 'id' (UUID) column se data fetch kar rahe hain
    try:
        response = supabase.table(TABLE_NAME).select('file_id, title, file_size').eq('id', movie_id_or_uuid).single().execute()
        
        # Check if response data is valid and has keys
        if response.data and 'file_id' in response.data:
            return response.data
        return None
        
    except Exception as e:
        # Agar Supabase mein row na mile ya koi error aaye
        print(f"Supabase lookup error for ID {movie_id_or_uuid}: {e}")
        return None

# NOTE: Agar aap 'id' column ko Integer Primary Key mein badal dete hain, 
# toh aap URL mein simple number use kar paenge.
