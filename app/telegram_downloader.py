import os
from telethon import TelegramClient
from app.storage_manager import StorageManager

class TelegramDownloader:
    def __init__(self, api_id, api_hash, storage_manager: StorageManager, session_name="my_session"):
        self.client = TelegramClient(session_name, api_id, api_hash)
        self.storage = storage_manager
        os.makedirs("downloads", exist_ok=True)  # תיקייה זמנית

    async def download_group_messages(self, group_link: str, limit=50):
        group = await self.client.get_entity(group_link)
        print(f"✅ Connected to group: {group.title}")

        async for message in self.client.iter_messages(group, limit=limit):
            msg_id = message.id
            user = message.sender_id
            text = message.text
            date = message.date
            media_files = []

            # ---- תמונות ----
            if message.photo:
                tmp_path = f"downloads/photo_{msg_id}.jpg"
                path = await message.download_media(file=tmp_path)
                if path and os.path.exists(path):
                    file_id = self.storage.save_file(path, {
                        "message_id": msg_id,
                        "user": user,
                        "category": "image",
                        "caption": text,
                        "date": date
                    })
                    os.remove(path)
                    media_files.append({"category": "image", "file_id": str(file_id)})
                else:
                    print(f"⚠️ Failed to download photo for message {msg_id}")

            # ---- וידאו/אודיו/מסמכים ----
            elif message.document:
                tmp_path = f"downloads/doc_{msg_id}"
                path = await message.download_media(file=tmp_path)
                if path and os.path.exists(path):
                    mime_type = message.document.mime_type or ""
                    if "video" in mime_type:
                        category = "video"
                    elif "audio" in mime_type:
                        category = "audio"
                    else:
                        category = "document"

                    file_id = self.storage.save_file(path, {
                        "message_id": msg_id,
                        "user": user,
                        "category": category,
                        "caption": text,
                        "date": date
                    })
                    os.remove(path)
                    media_files.append({"category": category, "file_id": str(file_id)})
                else:
                    print(f"⚠️ Failed to download document for message {msg_id}")

            # ---- טקסט בלבד ----
            if text and not media_files:
                tmp_path = f"downloads/text_{msg_id}.txt"
                with open(tmp_path, "w", encoding="utf-8") as f:
                    f.write(text)
                if os.path.exists(tmp_path):
                    file_id = self.storage.save_file(tmp_path, {
                        "message_id": msg_id,
                        "user": user,
                        "category": "text",
                        "caption": text,
                        "date": date
                    })
                    os.remove(tmp_path)
                    media_files.append({"category": "text", "file_id": str(file_id)})
                else:
                    print(f"⚠️ Failed to save text for message {msg_id}")

            print(f"Saved message {msg_id} with {len(media_files)} media files")
