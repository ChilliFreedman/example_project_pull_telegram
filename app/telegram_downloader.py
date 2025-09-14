import os
from telethon import TelegramClient, events
from collections import defaultdict
from app.storage_manager import StorageManager

class TelegramDownloader:
    def __init__(self, api_id, api_hash, storage_manager: StorageManager, session_name="my_session"):
        self.client = TelegramClient(session_name, api_id, api_hash)
        self.storage = storage_manager
        os.makedirs("downloads", exist_ok=True)
        self.album_cache = defaultdict(list)

    async def download_group_messages(self, group_link: str, limit=50):
        """מוריד הודעות קיימות מהקבוצה כולל אלבומים"""
        group = await self.client.get_entity(group_link)
        print(f"✅ Connected to group: {group.title}")

        async for message in self.client.iter_messages(group, limit=limit):
            album_id = getattr(message, "grouped_id", None)

            if album_id:
                self.album_cache[album_id].append(message)
                continue

            await self._process_single_message(message, group_link, message.text or "")

        # טיפול באלבומים אחרי שהורדנו את כל ההודעות
        for album_id, messages in self.album_cache.items():
            caption = ""
            for msg in messages:
                if msg.text:
                    caption = msg.text
                    break
            for msg in messages:
                await self._process_single_message(msg, group_link, caption)

        self.album_cache.clear()

    def start_listening(self, group_link):
        """מתחיל להאזין להודעות חדשות בזמן אמת"""
        @self.client.on(events.NewMessage(chats=group_link))
        async def handler(event):
            message = event.message
            album_id = getattr(message, "grouped_id", None)
            text = message.text or ""

            if album_id:
                self.album_cache[album_id].append(message)
                if len(self.album_cache[album_id]) > 1:
                    caption = ""
                    for msg in self.album_cache[album_id]:
                        if msg.text:
                            caption = msg.text
                            break
                    for msg in self.album_cache[album_id]:
                        await self._process_single_message(msg, group_link, caption)
                    self.album_cache.pop(album_id, None)
            else:
                await self._process_single_message(message, group_link, text)

        print(f"✅ Listening to group: {group_link} (press Ctrl+C to stop)")

    async def _process_single_message(self, message, group_link, text):
        msg_id = message.id
        user = message.sender_id
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
                    "group_link": group_link,
                    "category": "image",
                    "caption": text,
                    "date": date
                })
                os.remove(path)
                media_files.append({"category": "image", "file_id": str(file_id)})

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
                    "group_link": group_link,
                    "category": category,
                    "caption": text,
                    "date": date
                })
                os.remove(path)
                media_files.append({"category": category, "file_id": str(file_id)})

        # ---- טקסט בלבד ----
        if text and not media_files:
            tmp_path = f"downloads/text_{msg_id}.txt"
            with open(tmp_path, "w", encoding="utf-8") as f:
                f.write(text)
            if os.path.exists(tmp_path):
                file_id = self.storage.save_file(tmp_path, {
                    "message_id": msg_id,
                    "user": user,
                    "group_link": group_link,
                    "category": "text",
                    "caption": text,
                    "date": date
                })
                os.remove(tmp_path)
                media_files.append({"category": "text", "file_id": str(file_id)})

        print(f"Saved message {msg_id} with {len(media_files)} media files")
