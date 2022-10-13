import pathlib
from pathlib import Path

import asyncinotify
from asyncinotify import Inotify, Mask
import asyncio
from loguru import logger
import confuse
import hashlib
import aiofiles

from rabbitmq import Rabbitmq

CONFIG = None

async def hash_file(file_path:pathlib.Path):
    md5 =hashlib.md5()

    async with aiofiles.open(str(file_path), mode="r") as fb:
        while True:
            data = await fb.read(CONFIG["observer"]["buf_size"])
            if not data:
                break
            md5.update(data)
        return md5.hexdigest()

async def extract_event_data(event: asyncinotify.Event):
    return {
        "event": event.name,
        "file_path": str(event.path.parent),
        "filename": str(event.path.name)
    }

async def main():

    # Context manager to close the inotify handle after use
    with Inotify() as inotify:
        # Adding the watch can also be done outside of the context manager.
        # __enter__ doesn't actually do anything except return self.
        # This returns an asyncinotify.inotify.Watch instance

        inotify.add_watch(CONFIG["observer"]["watched_folder"], Mask.CREATE | Mask.MOVED_TO)
        # inotify.add_watch('/tmp', Mask.ACCESS | Mask.MODIFY | Mask.OPEN | Mask.CREATE | Mask.DELETE | Mask.ATTRIB | Mask.CLOSE | Mask.MOVE | Mask.ONLYDIR)
        # Iterate events forever, yielding them one at a time
        async for event in inotify:
            _rabbitmq = await Rabbitmq(config=CONFIG).run()
            message = []
            # Events have a helpful __repr__.  They also have a reference to
            # their Watch instance.
            logger.info(f" Event : {event.mask}; Name : {event.name}")
            results = await asyncio.gather(
                extract_event_data(event),
                hash_file(event.path)
            )
            logger.info(f"message {message}")
            logger.info(f"result {results}")
            event_data = results[0]
            message = {
                "md5_hash": results[1],
                "file_path": Path(event_data["file_path"], event_data["filename"])
            }

            await _rabbitmq.publish_message(message)
            logger.info(f"Publish message : {message}")

            # the contained path may or may not be valid UTF-8.  See the note
            # below

if __name__ == '__main__':
    config = confuse.Configuration("observer", __name__)
    config.set_file("/app/config.yaml")
    CONFIG = config.get()
    try:
        logger.info("Start observer")
        asyncio.run(main(), debug=True)
    except KeyboardInterrupt:
        logger.info('shutting down')