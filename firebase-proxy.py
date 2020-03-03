#!/usr/bin/env python3
#
# Proxy for Firebase logging
#
#A few settings
MAXQUEUE = 100 #Maximum number of pending data to be stored
NBPROC = 5 #Number of logging process to run
POLICY = "first"  #Define what to do when the queue is full:
                  #     "first": drop the first element in the queue
                  #     "last":  drop the last element in the queue
                  #     "wait":  wait until there is space
                  # any other value is considered to be "last"

FBSECRET = "AIzaSyB96PxwtBpkNhhJdaLBwgl708VHsw10qYE"
FBURL = "https://altotech-3a96e.firebaseio.com"
FBTARGET="Naplap"
PORT = 8088
from firebase import firebase as pfb
from aiohttp import web
import asyncio as aio
import logging as _log
import sys


class Logger:
    
    def __init__(self,myid):
        self.fbapp = pfb.FirebaseApplication(FBURL, authentication=None)
        #self.authenticate()
        self.data = None
        self.myid = myid
        
    def authenticate(self):
        self.fbapp.authentication = pfb.FirebaseAuthentication(FBSECRET, 'arm@gmail.com', extra={'id': 123})
        
    async def logging(self):
        _log.debug("Logging to {} -> {} value {}".format("/{}".format(FBTARGET),self.data["mac address"],self.data["data"]))
        try:
            self.fbapp.put_async("/{}".format(FBTARGET+"/"+self.data["target"]),self.data["mac address"],self.data["data"],callback=self.async_cb)
        except:
            self.fbapp.put_async("/{}".format(FBTARGET+"/devices"),self.data["mac address"],self.data["data"],callback=self.async_cb)
        await aio.sleep(1)
        tout = 6
        while self.data:
            await aio.sleep(0.5)
            tout -= 1
            if tout:
                continue
            _log.warning("Time out trying to send data to Firebase. data dropped.")
            self.data = None
           
    
    def async_cb(self,resp):
        """Log data to Firebase by retrieving it from the queue"""
        _log.debug("Response is {}".format(resp))
        #if 'error' in resp and resp['error'] == 'error':
            #self.authenticate()
        #else:
        self.data=None
        
      
    async def runme(self):
        global dataqueue
        goon = True
        while goon:
            try:
                if self.data is None:
                    _log.debug("Logger {} waiting for data. Len is {}, data is {}".format(self.myid,dataqueue.qsize(),self.data))
                    self.data = await dataqueue.get()
                    _log.debug("Logger {} got {}".format(self.myid, self.data))
                else:
                    _log.debug("Logger {} trying again {}".format(self.myid, self.data))
                await self.logging()
            except aio.CancelledError as e:
                goon = False
            except Exception as ex:
                _log.debug("Something went bad. {}".format(ex))
                
                

async def submit(request):
    global dataqueue
    
    data = await request.json()
    _log.debug("Request is {}".format(data))
    if dataqueue.full():
        if POLICY == "first":
            tbddata = await dataqueue.get()
        elif POLICY == "wait":
            pass
        else:
            return web.Response(text="OK")
    await dataqueue.put(data)
    return web.Response(text="OK")
  
async def shutdown(app):
    _log.debug("Shutdown signal received")
    for tsk in app['loggers']:
        tsk.cancel()
  
async def startup(app):
    _log.debug("Start signal received")
    for x in range(NBPROC):
        _log.debug("Starting logger {}".format(x))
        newlog = Logger(x)
        newtsk = aio.ensure_future(newlog.runme())
        app['loggers'].append(newtsk)
    await aio.sleep(0)
    
    
async def startmeup(app):
    global runner
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, 'localhost', PORT)
    await site.start()
  
  
_log.basicConfig(
            level=_log.DEBUG,
            format='%(levelname)7s: %(message)s',
            stream=sys.stderr,
        )
loop = aio.get_event_loop()
dataqueue = aio.Queue(MAXQUEUE)
 
runner = None
app = web.Application()
app['loggers'] = []
app.add_routes([web.post('/submit', submit)])
app.on_shutdown.append(shutdown)
app.on_startup.append(startup)
#whandler = app.make_handler()
#wapp=loop.create_server(whandler,host=guiwscfg["host"],port=guiwscfg["port"],ssl=ssl_context)
loop.run_until_complete(startmeup(app))
try:
    loop.run_forever()
except  KeyboardInterrupt:
    _log.debug("\nExiting at user's request")
finally:
    loop.run_until_complete(runner.cleanup())
    loop.run_until_complete(app.shutdown())
    #loop.run_until_complete(whandler.finish_connections(60.0))
    loop.run_until_complete(app.cleanup())
    loop.close()
