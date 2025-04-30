from db_manager import DatabaseManager
from player import Player

# Simple test of the database manager if run directly
if __name__ == "__main__":
    # Create an instance of the database manager
    db_manager = DatabaseManager()

    player1 = Player("Niklas", "pico_id123", db_manager)
    player2 = Player("Bob", "pico_id67868", db_manager)
    player3 = Player("Anton", "pico_id4564598", db_manager)

    # Load player from database
    loaded_player = Player.load_from_db(player2.player_id, db_manager)
    print(loaded_player)

    # Get all players
    print(db_manager.get_all_players())
