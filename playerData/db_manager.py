import os
import sqlite3


# A class to manage SQLite database operations for the Player class.
class DatabaseManager:

    def __init__(self, db_name="player_data.db"):
        # Ensure the database is always created/accessed in the playerData directory
        player_data_dir = os.path.dirname(os.path.abspath(__file__))
        self.db_name = os.path.join(player_data_dir, db_name)
        self.conn = None
        self.cursor = None

        # Create database if it doesn't exist
        self.connect()
        self.create_tables()
        self.close()

    def connect(self):

        self.conn = sqlite3.connect(self.db_name)
        self.conn.row_factory = sqlite3.Row  # Enable row factory for dict-like access
        self.cursor = self.conn.cursor()

    def close(self):

        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None

    def create_tables(self):
        # Create players table
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS players (
            player_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            pico_id TEXT NOT NULL,
            created_at DATETIME NOT NULL
        )
        ''')

        self.conn.commit()

    def add_player(self, player_id, name, pico_id, created_at):

        try:
            self.connect()
            self.cursor.execute(
                "INSERT INTO players (player_id, name, pico_id, created_at) VALUES (?, ?, ?, ?)",
                (player_id, name, pico_id, created_at.isoformat())
            )
            self.conn.commit()
            return True
        except sqlite3.IntegrityError as e:
            print(f"Error adding player: {e}")
            return False
        finally:
            self.close()

    # Get player information by ID. Returns dictionary or None
    def get_player(self, player_id):
        try:
            self.connect()
            self.cursor.execute("SELECT player_id, name, pico_id, created_at FROM players WHERE player_id = ?", (player_id,))
            player = self.cursor.fetchone()

            if player:
                return {
                    "player_id": player["player_id"],
                    "name": player["name"],
                    "pico_id": player["pico_id"],
                    "created_at": player["created_at"]
                }
            return None
        finally:
            self.close()

    # Give player details by Pico ID
    def get_player_by_pico_id(self, pico_id):
        try:
            self.connect()
            self.cursor.execute("SELECT player_id, name, pico_id, created_at FROM players WHERE pico_id = ?",
                                (pico_id,))
            player = self.cursor.fetchone()

            if player:
                return {
                    "player_id": player["player_id"],
                    "name": player["name"],
                    "pico_id": player["pico_id"],
                    "created": player["created_at"]
                }
            return None
        finally:
            self.close()

    # Returns a list of player dictionaries
    def get_all_players(self):

        try:
            self.connect()
            self.cursor.execute("SELECT player_id, name, pico_id, created_at FROM players")
            players = self.cursor.fetchall()
            return [
                {
                    "player_id": p["player_id"],
                    "name": p["name"],
                    "pico_id": p["pico_id"],
                    "created_at": p["created_at"]
                }
                for p in players
            ]
        finally:
            self.close()

    def delete_player(self, player_id):

        try:
            self.connect()
            # delete a player by id from players table
            self.cursor.execute("DELETE FROM players WHERE player_id = ?", (player_id,))
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Error deleting player: {e}")
            return False
        finally:
            self.close()
