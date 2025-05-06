from db_manager_pg import DatabaseManager
from player import Player
from MQTT.publisher import hr_data


if __name__ == "__main__":
    # Create an instance of the database manager
    db_manager = DatabaseManager()

    # Get Pico_ID from hr_data in MQTT.publisher file
    data = hr_data()
    pico_id = data["Pico_ID"]

    # players.name instead of string name when names are coming from the frontend
    player = Player("Merlin", pico_id, db_manager)


    # Load player from database
    loaded_player = Player.load_from_db(player.player_id, db_manager)
    print(loaded_player)

    # Get all players
    print(db_manager.get_all_players())
