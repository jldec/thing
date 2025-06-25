// Original code copied from https://github.com/threepointone/durable-chat
// MIT License (c) 2024 Sunil Pai
import { type Connection, type ConnectionContext, Server, type WSMessage } from 'partyserver'
import { nanoid } from 'nanoid'
import { EventSourceParserStream } from 'eventsource-parser/stream'

import type { ChatMessage, Message } from './shared'
import type { PageData } from './types'

type Env = {
  AI: Ai
  PAGEDATA_CACHE: KVNamespace
  ENVIRONMENT: string
}

export class Party extends Server<Env> {
  static options = {
    hibernate: true
  }
  messages = [] as ChatMessage[]
  pageData: PageData | undefined = undefined

  async onStart() {
    // TODO: protect against paths with _
    const path = this.name.replace(/_/g, '/')
    console.log('Party started on', path)
    const savedMessages = await this.ctx.storage.get<ChatMessage[]>('messages')
    if (savedMessages?.length) {
      this.messages = savedMessages
      console.log(`Loaded ${savedMessages.length} messages from storage`)
    }
    if (!this.pageData) {
      if (path) {
        const cachedContent = await this.env.PAGEDATA_CACHE.get(path)
        if (cachedContent !== null) {
          this.pageData = JSON.parse(cachedContent) as PageData
        }
      }
    }
  }

  sendMessage(connection: Connection, message: Message) {
    connection.send(JSON.stringify(message))
  }

  broadcastMessage(message: Message, exclude?: string[]) {
    this.broadcast(JSON.stringify(message), exclude)
  }

  async onConnect(connection: Connection, ctx: ConnectionContext) {
    this.sendMessage(connection, {
      type: 'all',
      messages: this.messages
    })
  }

  async onMessage(connection: Connection, message: WSMessage) {
    // let's broadcast the raw message to everyone else
    this.broadcast(message)
    // let's update our local messages store
    const parsed = JSON.parse(message as string) as Message

    if (parsed.type === 'add') {
      // add the message to the local store
      this.messages.push(parsed)

      // TODO: better logging
      const log = this.pageData?.path + '\n' + parsed.content
      console.log('message', log)
      if (this.env.ENVIRONMENT === 'prod') {
        fetch('https://my-email-worker.jldec.workers.dev/', {
          method: 'POST',
          headers: {
            'Content-Type': 'text/plain'
          },
          body: log
        })
      }

      // let's ask AI to respond as well for fun
      const aiMessage = {
        id: nanoid(8),
        content: '...',
        user: 'AI',
        role: 'assistant'
      } as const

      this.broadcastMessage({
        type: 'add',
        ...aiMessage
      })

      const links =
        `\n\n# ${this.pageData?.attrs?.title || 'Links'}\n\n` +
          this.pageData?.dir
            ?.map(
              (d) =>
                `- [${d.attrs?.title || d.path.split('/').filter(Boolean).join(' ') || 'Home'}](${
                  d.path
                })`
            )
            .join('\n') || ''

      const systemMessage = {
        role: 'system',
        content:
          'You are a helpful assistant. Respond concisely, with levity and respect, about the following content: ' +
          (this.pageData?.md || '') +
          links
      }

      try {
        // @ts-expect-error
        const aiMessageStream = (await this.env.AI.run('@cf/meta/llama-3.1-8b-instruct-fp8-fast', {
          stream: true,
          max_tokens: 2048,
          messages: [
            systemMessage,
            ...this.messages.map((m) => ({
              content: m.content,
              role: m.role
            }))
          ] as RoleScopedChatInput[]
        })) as ReadableStream

        this.messages.push(aiMessage)

        const eventStream = aiMessageStream
          .pipeThrough(new TextDecoderStream())
          .pipeThrough(new EventSourceParserStream())

        // We want the AI to respond to the message in real-time
        // so we're going to stream every chunk as an "update" message

        let buffer = ''

        for await (const event of eventStream) {
          if (event.data !== '[DONE]') {
            // let's append the response to the buffer
            buffer += JSON.parse(event.data).response
            // and broadcast the buffer as an update
            this.broadcastMessage({
              type: 'update',
              ...aiMessage,
              content: buffer + '...' // let's add an ellipsis to show it's still typing
            })
          } else {
            // the AI is done responding
            // we update our local messages store with the final response
            this.messages = this.messages.map((m) => {
              if (m.id === aiMessage.id) {
                return {
                  ...m,
                  content: buffer
                }
              }
              return m
            })
            this.saveMessages()

            // let's update the message with the final response
            this.broadcastMessage({
              type: 'update',
              ...aiMessage,
              content: buffer
            })
          }
        }
      } catch (err) {
        console.error(err)
      }
    } else if (parsed.type === 'update') {
      // update the message in the local store
      const index = this.messages.findIndex((m) => m.id === parsed.id)
      this.messages[index] = parsed
    } else if (parsed.type === 'clear') {
      // clear the local store
      this.messages = []
      this.saveMessages()
    }
  }
  saveMessages() {
    this.ctx.waitUntil(this.ctx.storage.put('messages', this.messages))
    console.log(`Saved ${this.messages.length} messages to storage`)
  }
}
