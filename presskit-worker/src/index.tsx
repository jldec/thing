import { Hono } from 'hono'
import { jsxRenderer } from 'hono/jsx-renderer'
import { memo } from 'hono/jsx'
import { serveStatic } from 'hono/cloudflare-workers'
import { StatusCode } from 'hono/utils/http-status'
import { raw } from 'hono/html'
import { parseFrontmatter } from './parse/frontmatter'
import { parseMarkdown } from './parse/markdown'

// https://hono.dev/docs/middleware/builtin/jsx-renderer#extending-contextrenderer
declare module 'hono' {
	interface ContextRenderer {
		(content: string | Promise<string>, props: { title?: string; htmlContent?: string }): Response
	}
}

// @ts-expect-error
// TODO - figure out if this is required.
// followed: https://hono.dev/docs/getting-started/cloudflare-workers#serve-static-files
// see also: https://github.com/honojs/hono/issues/1127
import manifest from '__STATIC_CONTENT_MANIFEST'

type Bindings = {
	page_cache: KVNamespace
	AI: Ai
	GH_PAT: string
}

const app = new Hono<{ Bindings: Bindings }>()

app.get('/static/*', serveStatic({ root: './', manifest }))

const fileUrlPrefix = 'https://raw.githubusercontent.com/jldec/presskit/main/content'
const indexFile = 'index.md'

type Content = {
	statusCode: StatusCode
	attrs: any
	html: string
	summary?: AiSummarizationOutput
}

let homeContent: Content | null = null

async function getContent(url: string): Promise<Content> {
	try {
		const response = await fetch(url)
		const parsedContent = parseFrontmatter(await response.text())

		return {
			statusCode: response.status as StatusCode,
			attrs: parsedContent.attrs,
			html: parseMarkdown(parsedContent.body)
		}
	} catch (error) {
		return {
			statusCode: 500 as StatusCode,
			attrs: {},
			html: (error as any).message as string
		}
	}
}

const Navbar = async () => {
	if (homeContent === null) await getHomeContent()
	return (
		<div class="navbar justify-center bg-primary text-primary-content">
			{homeContent?.attrs.nav?.map((item: any) => (
				<a class="link px-2" href={item.link}>
					{item.text ?? item.link}
				</a>
			))}
		</div>
	)
}

// https://hono.dev/docs/api/context#render-setrenderer
// https://hono.dev/docs/helpers/html#insert-snippets-into-jsx
app.use(
	jsxRenderer(({ children, title, htmlContent }) => {
		return (
			<html lang="en">
				<head>
					<meta charset="utf-8" />
					<meta name="viewport" content="width=device-width, initial-scale=1" />
					<title>{title ?? 'Presskit'}</title>
					<link href="static/css/style.css" rel="stylesheet" />
				</head>
				<body>
					<Navbar />
					<div class="p-4">
						<div class="prose mx-auto ">
							{children}
							{raw(htmlContent)}
						</div>
					</div>
				</body>
			</html>
		)
	})
)

async function getHomeContent() {
	homeContent = await getContent(`${fileUrlPrefix}/${indexFile}`)
	homeContent.attrs = {
		nav: [
			{
				text: 'home',
				link: '/'
			},
			{
				text: 'new-thing',
				link: '/new-thing'
			},
			{
				text: 'multi-page',
				link: '/multi-page'
			},
			{
				text: 'tailwind',
				link: '/tailwind'
			},
			{
				text: 'summarize',
				link: '/summarize'
			},
			{
				text: 'daisyUI',
				link: '/daisyui'
			}
		]
	}
	console.log(JSON.stringify(homeContent.attrs))
}

app.get('/', async (c) => {
	if (homeContent === null) await getHomeContent()
	return c.render('', { htmlContent: homeContent?.html, title: homeContent?.attrs?.title })
})

app.get('/tree', async (c) => {
	let cachedTree = await c.env.page_cache.get('TREE')
	if (cachedTree !== null) {
		return c.json(JSON.parse(cachedTree))
	} else return c.notFound()
})

const treeUrl = 'https://api.github.com/repos/jldec/presskit/git/trees/HEAD?recursive=TRUE'

app.post('/tree', async (c) => {
	let resp = await fetch(treeUrl, {
		headers: {
			Accept: 'application/vnd.github+json',
			Authorization: `Bearer ${c.env.GH_PAT}`,
			'X-GitHub-Api-Version': '2022-11-28',
			'User-Agent': 'presskit-worker'
		}
	})
	if (resp.ok) {
		await c.env.page_cache.put('TREE', JSON.stringify(await resp.json()))
		return c.text('OK')
	} else {
		c.status(resp.status as StatusCode)
		return c.text(resp.statusText)
	}
})

// Translate request path into file URL, including .md extension
// https://hono.dev/docs/api/routing#including-slashes
app.get('/:path{.+$}', async (c) => {
	const { path } = c.req.param()
	let content: Content
	const cachedContent = await c.env.page_cache.get(path)
	if (cachedContent !== null) {
		content = JSON.parse(cachedContent) as Content
	} else {
		content = await getContent(`${fileUrlPrefix}/${path}.md`)
		c.executionCtx.waitUntil(summarizeAndCache(c.env, path, content))
	}
	c.status(content.statusCode)
	return c.render(
		<>
			<h2>AI Summary</h2>
			{content.summary?.summary ?? ' No summary yet.'}
			<hr />
		</>,
		{ htmlContent: content.html, title: content.attrs?.title }
	)
})

async function summarizeAndCache(env: Bindings, key: string, content: Content) {
	content.summary = await env.AI.run('@cf/facebook/bart-large-cnn', {
		input_text: content.html,
		max_length: 50
	})
	// console.log('summarized content', JSON.stringify(content,null,2))
	return env.page_cache.put(key, JSON.stringify(content))
}

export default app
