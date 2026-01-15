cd C:\Users\samua\Desktop\Daz\joeyjones\backend
python seed_openfootball_clubs.py
python seed_football_assets.py
uvicorn server:application --host 0.0.0.0 --port 8001

cd C:\Users\samua\Desktop\Daz\joeyjones\frontend
yarn install
yarn start

cd C:\Users\samua\Desktop\Daz\joeyjones\tests
python multi_league_stress_test.py --leagues 1 --users 2 --teams 2 --url http://localhost:8001
python multi_league_stress_test.py --leagues 1 --users 8 --teams 4 --url http://127.0.0.1:8001


python multi_league_stress_test.py --leagues 1 --users 2 --teams 2 --url https://draft-kings-mobile.emergent.host