import pandas as pd

def extract_spotify_data(file_path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
        print(f"Dataset de Spotify cargado correctamente desde {file_path}")
        print(f"Shape: {df.shape}")
        return df
    except FileNotFoundError:
        print(f"Error: archivo no encontrado en {file_path}")
    except Exception as e:
        print(f"Error al cargar el CSV: {e}")