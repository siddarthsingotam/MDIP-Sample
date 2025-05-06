import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime


class DatabaseManager:
    def __init__(self, dbname="defaultdb", user="avnadmin", password="AVNS_hECP7FXEij7npcktiKj", host="pg-icehockey-player-metrics-andydeshko-a139.l.aivencloud.com", port=25420):
        self.connection_params = {
            "dbname": dbname,
            "user": user,
            "password": password,
            "host": host,
            "port": port
        }
        self.conn = None
        self.cursor = None

        # Initialize the DB
        self.connect()
        self.create_tables()
        self.close()

    def connect(self):
        self.conn = psycopg2.connect(**self.connection_params)
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None

    def create_tables(self):
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS players (
            player_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            pico_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        ''')
        self.conn.commit()

    def add_player(self, player_id, name, pico_id, created_at):
        try:
            self.connect()
            self.cursor.execute(
                "INSERT INTO players (player_id, name, pico_id, created_at) VALUES (%s, %s, %s, %s)",
                (player_id, name, pico_id, created_at)
            )
            self.conn.commit()
            return True
        except psycopg2.IntegrityError as e:
            print(f"Error adding player: {e}")
            self.conn.rollback()
            return False
        finally:
            self.close()

    def get_player(self, player_id):
        try:
            self.connect()
            self.cursor.execute("SELECT * FROM players WHERE player_id = %s", (player_id,))
            player = self.cursor.fetchone()
            return dict(player) if player else None
        finally:
            self.close()

    def get_player_by_pico_id(self, pico_id):
        try:
            self.connect()
            self.cursor.execute("SELECT * FROM players WHERE pico_id = %s", (pico_id,))
            player = self.cursor.fetchone()
            return dict(player) if player else None
        finally:
            self.close()

    def get_all_players(self):
        try:
            self.connect()
            self.cursor.execute("SELECT * FROM players")
            players = self.cursor.fetchall()
            return [dict(p) for p in players]
        finally:
            self.close()

    def delete_player(self, player_id):
        try:
            self.connect()
            self.cursor.execute("DELETE FROM players WHERE player_id = %s", (player_id,))
            self.conn.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error deleting player: {e}")
            self.conn.rollback()
            return False
        finally:
            self.close()
