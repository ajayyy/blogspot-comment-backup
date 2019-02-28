import json, asyncio, aiohttp, logging, traceback
from time import perf_counter

import sys

sys.path.insert(0, './fetch/')

from fetch.posts import get_blog_posts
from fetch.comments import get_comments_from_post
from fetch.util import get_url_path

from batch_file import BatchFile

# import string, random

# sharing state between downloaders is just too hard without global variables
# they will have to do for now

class PostsDownloader:

	log_cooldown = 0

	def __init__(self, blog_posts, batch_file, exclude_limit, starting_post=0, downloader_count=10, graceful_killer=None):
		self.blog_posts = blog_posts
		self.batch_file = batch_file

		if graceful_killer:
			self.graceful_killer = graceful_killer

		self.exclude_limit = exclude_limit
		self.starting_post = starting_post
		self.downloader_count = downloader_count

		self.time_start = perf_counter()

		self.posts_finished = 0
		# self.chars = string.ascii_letters + string.digits

		self.downloaders_finished = 0
		self.downloaders_should_pause = False
		self.downloaders_paused = 0
		self.downloader_tasks = []

		self.restarting_session = False

		self.session_headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:65.0) Gecko/20100101 Firefox/65.0"}
		self.session_timeout = aiohttp.ClientTimeout(total=20)
		self.session_connector = aiohttp.TCPConnector(limit=30)
		self.session = aiohttp.ClientSession(connector=self.session_connector, headers=self.session_headers, timeout=self.session_timeout, connector_owner=False)

		self.queue = []
		for post in self.blog_posts[self.starting_post:]:
			self.queue.append(post)

		for i in range(self.downloader_count):
			prefix = "0" if i < 10 else ""
			downloader_task = asyncio.create_task(self.downloader(f"downloader-{prefix}{i}", self.batch_file, self.queue))
			self.downloader_tasks.append(downloader_task)


	async def start(self):
		t0 = perf_counter()
		await asyncio.gather(*self.downloader_tasks)
		duration = perf_counter() - t0
		print("Saved %s posts in %s seconds" % (self.posts_finished, format(duration, '.2f')))
		await self.session.close()
		await self.session_connector.close()

	async def downloader(self, name, batch_file, queue):

		worker_posts_downloaded = 0

		paused = False

		while len(queue) > 0:
			if not self.downloaders_should_pause and (self.starting_post + self.posts_finished < len(self.blog_posts)):
				url = queue.pop()
				try:
					if paused:
						paused = False
						self.downloaders_paused -= 1
						print(f"{name} | Resuming from rate limit pause")
						self.print_downloader_status(name)
					await self.download_post(name, url)
					worker_posts_downloaded += 1
				except json.decoder.JSONDecodeError as e:
					try:
						print(f"{name} | Pause reason: {traceback.format_exc()}")
						print(f"{name} | Paused due to rate limit")
						self.print_downloader_status(name)
						if not self.downloaders_should_pause:
							self.downloaders_should_pause = True
						# Add the url back to the queue for another task do pick up
						self.requeue_url(name, url)
					except Exception as e:
						exit(e)
				except Exception as e:
					exit(e)

			else:
				await asyncio.sleep(5)
				if not paused:
					paused = True
					self.downloaders_paused += 1
				else:
					print(f"{name} | Waiting for all downloaders to pause")
					self.print_downloader_status(name)

				if not self.restarting_session and self.downloaders_should_pause and self.downloaders_paused >= (self.downloader_count - self.downloaders_finished):
					self.restarting_session = True
					# sleep for a bit so we don't resume right after hitting the captcha page
					await asyncio.sleep(10)
					print("All downloaders paused, restarting session")
					self.print_downloader_status(name)
					await self.session.close()
					self.session = aiohttp.ClientSession(connector=self.session_connector, headers=self.session_headers, timeout=self.session_timeout, connector_owner=False)
					self.downloaders_should_pause = False
					self.restarting_session = False

		self.downloaders_finished += 1
		print(f"{name} DONE | Posts Downloaded: {worker_posts_downloaded}")
		self.print_downloader_status(name)

	async def download_post(self, name, url):
		try:
			comments = await get_comments_from_post(url, self.session, get_all_pages=True, get_replies=True, get_comment_plus_ones=True, get_reply_plus_ones=True)

			first_post = self.posts_finished == 0
			self.batch_file.add_blog_post(url, comments, first_post)

			# include a random string to prevent file name collisions
			# random_chars = "".join(random.choices(chars, k=7))
			# file_path = f"../output/{get_url_path(url)}_{random_chars}.json"
			# with open(file_path, "w") as file:
			# 	file.write(json.dumps({"url": url, "comments": comments}))

			total_time = perf_counter() - self.time_start
			if PostsDownloader.log_cooldown >= 20 or self.downloaders_should_pause or self.restarting_session:
				self.print_downloader_progress(name, total_time)
				self.print_downloader_status(name)
				PostsDownloader.log_cooldown = 0
			else:
				PostsDownloader.log_cooldown += 1
			self.posts_finished += 1
		except (
				asyncio.TimeoutError, 
				aiohttp.client_exceptions.ServerDisconnectedError, 
				aiohttp.client_exceptions.ClientOSError,
				aiohttp.client_exceptions.ClientConnectorError,
				TypeError
			) as e:

			print(f"{name} | Retry reason: {traceback.format_exc()}")

			print(f"{name} | {self.batch_file.file_name} | An error occurred during the request, requeuing post in 5 seconds")
			await asyncio.sleep(5)
			self.requeue_url(name, url)

	def requeue_url(self, name, url):
		print(f"{name} | Requeuing post: \'{get_url_path(url)}\'")
		self.queue.append(url)

	def print_downloader_progress(self, name, total_time):
		print(f"{name} | [PROGRESS] {self.batch_file.file_name} | Post {self.starting_post + self.posts_finished + 1}/{len(self.blog_posts)} | Total time running: {format(total_time, '.2f')}s")

	def print_downloader_status(self, name):
		print(f"{name} | downloaders_paused: {self.downloaders_paused} downloaders_finished: {self.downloaders_finished}\n")


async def main():

	# logging.basicConfig(format="%(message)s", level=logging.INFO)
	# 
	# async with aiohttp.ClientSession() as session:

		# blog = "https://dev.blogspot.com"
		# blog_posts = await get_blog_posts(blog, 450, session)

	# Use a file of post urls for faster debugging
	with open("../test_data/googleblog_posts.json", "r") as file:
		# with open("../test_data/blogger_googleblog.json", "r") as file2:

		batch_file = BatchFile("../output/", 120312)
		
		blog_posts = json.loads(file.read())
		print(blog_posts)
		# blog_posts_2 = json.loads(file.read())

		batch_file.start_blog(1, "googleblog", "googleblog.blogspot.com", "a", True)
		dl = PostsDownloader(blog_posts, batch_file, 450)
		await dl.start()
		batch_file.end_blog()

		# batch_file.start_blog(1, "clean", "clean.blogspot.com", "a", False)
		# await download_blog(blog_posts_2, batch_file, __exclude_limit=450, __starting_post=3300)
		# batch_file.end_blog()

		batch_file.end_batch()

		# batch_file_2 = BatchFile("../output/", 384753)
		# blog_posts_2 = json.loads(file.read())

		# batch_file_2.start_blog(1, "buzz", "buzz.blogspot.com", "a", True)
		# await download_blog(blog_posts_2, batch_file_2, __starting_post=3300)
		# batch_file_2.end_blog()
		# batch_file_2.end_batch()



if __name__ == '__main__':
	asyncio.run(main())