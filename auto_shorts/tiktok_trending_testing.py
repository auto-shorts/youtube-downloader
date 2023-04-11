from tiktokapipy.api import TikTokAPI

if __name__ == "__main__":
    with TikTokAPI() as api:
        challenge = api.challenge("sidemen", video_limit=20)
        for vid in challenge.videos._light_models:
            print(f"{vid.id} {vid.create_time}")
