import uuid
from datetime import datetime
from db_manager import DatabaseManager

class Player:

    def __init__(self, name, pico_id, db_manager=None):

        if not isinstance(name, str) or not name.strip():
            raise ValueError("Player name must be a non-empty string")
        if not isinstance(pico_id, str) or not pico_id.strip():
            raise ValueError("Pico ID must be a non-empty string")

        self.player_id = str(uuid.uuid4())  # Generate unique ID using uuid
        self.name = name
        self.pico_id = pico_id.strip()
        self.created_at = datetime.now()

        # Set up database manager
        self.db_manager = db_manager or DatabaseManager()

        # Store player in database
        success = self.db_manager.add_player(
            self.player_id,
            self.name,
            self.pico_id,
            self.created_at
        )
        if success:
            print(
                f"Player {self.name} (ID: {self.player_id}, Pico ID: {self.pico_id}) created and stored in database at {self.created_at}")
        else:
            raise RuntimeError(f"Failed to store player {self.name} in database")

    # Load a player from the database by player_id.
    @classmethod
    def load_from_db(cls, player_id, db_manager=None):

        if not isinstance(player_id, str):
            raise TypeError("Player ID must be a string")

        db = db_manager or DatabaseManager()
        player_data = db.get_player(player_id)

        if not player_data:
            print(f"No player found with ID: {player_id}")
            return None

        # Create player instance without storing it again
        player = cls.__new__(cls)
        player.player_id = player_data["player_id"]
        player.name = player_data["name"]
        player.pico_id = player_data["pico_id"]
        player.created_at = datetime.fromisoformat(player_data["created_at"])
        player.db_manager = db

        print(f"Player {player.name} (ID: {player.player_id}) loaded from database")
        return player

    def __str__(self):

        return f"Player(name={self.name}, id={self.player_id}, pico_id={self.pico_id}, created_at={self.created_at})"
