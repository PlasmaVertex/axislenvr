# axislenvr
LowCost NVR for Axis companion LE cameras

Using this command ffmpeg will save mkv video files at each quarter (15 mins) using the high quality fullhd video profile: 
```
ffmpeg -hide_banner -y -loglevel error -rtsp_transport tcp -use_wallclock_as_timestamps 1 -i "rtsp://user:password@192.168.0.14:554/axis-media/media.amp?videocodec=h265&streamprofile=ACC_high&Axis-Orig-Sw=true" -vcodec copy -acodec copy -f segment -reset_timestamps 1 -segment_time 900 -segment_format mkv -segment_atclocktime 1 -strftime 1 %Y%m%dT%H%M%S.mkv
```

Running parallel with the previous one, this command will save the motion events to a logfile in xml format:

```
openRTSP -b 400000 -K -L -t "rtsp://user:password@192.168.0.14:554/axis-media/media.amp?Axis-Orig-Sw=true&video=0&audio=0&event=on&eventtopic=onvif:VideoAnalytics//." > AxisEvents.log
```
