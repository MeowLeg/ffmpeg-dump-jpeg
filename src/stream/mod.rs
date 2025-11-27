use super::*;

pub fn stream(
    cfg: &Config,
    stream_url: &str,
    // rds: &Client,
    stream_md5_val: &str,
    uuid: &str,
    project_uuid: &str,
    organization_uuid: &str,
) -> Result<()> {
    // 初始化FFmpeg
    ffmpeg_next::init().context("FFmpeg初始化失败")?;

    // 打开RTMP输入流
    let mut ictx = format::input(&stream_url).context("无法打开RTMP流")?;

    // 查找视频流
    let video_stream_index = ictx
        .streams()
        .best(media::Type::Video)
        .ok_or_else(|| anyhow::anyhow!("找不到视频流"))?
        .index();

    // 获取视频流
    let stream = ictx
        .streams()
        .find(|s| s.index() == video_stream_index)
        .ok_or_else(|| anyhow::anyhow!("无法获取指定索引的视频流"))?;

    // 创建解码器上下文
    let dctx = codec::context::Context::from_parameters(stream.parameters())
        .context("创建解码器上下文失败")?;
    // 创建缩放上下文，用于将YUV帧转换为RGB

    // 帧计数器和计时器
    let mut frame_count: u128 = 0;

    let mut v_dctx = dctx.decoder().video()?;
    println!(
        "format is {:?}, width is {}, height is {}",
        v_dctx.format(),
        v_dctx.width(),
        v_dctx.height()
    );

    let mut scaler = scaling::Context::get(
        v_dctx.format(),
        v_dctx.width(),
        v_dctx.height(),
        ffmpeg_next::format::Pixel::RGB24,
        v_dctx.width(),
        v_dctx.height(),
        scaling::Flags::BILINEAR,
    )
    .context("无法创建缩放上下文")?;

    // 处理输入包
    let start_time = Instant::now();
    // let mut loop_num = 0;
    // loop {
    for (stream, packet) in ictx.packets() {
        if stream.index() == video_stream_index {
            // 时间戳
            // 将包发送到解码器
            match v_dctx.send_packet(&packet).context("发送数据包失败") {
                Ok(_) => {}
                Err(e) => {
                    println!("发送数据包错误: {e:?}");
                    continue;
                }
            };

            // 从解码器接收帧
            let mut decoded_frame = frame::Video::empty();
            while let Ok(()) = v_dctx.receive_frame(&mut decoded_frame) {
                frame_count += 1;

                // 示例：打印帧信息
                if frame_count.is_multiple_of(cfg.frame_interval_count as u128) {
                    let pts = decoded_frame.pts().unwrap_or(0);
                    let timebase = stream.time_base();
                    let timebase_num = timebase.numerator() as i64;
                    let timebase_den = timebase.denominator() as i64;

                    // 自定义帧处理逻辑...
                    match process_frame(
                        &mut scaler,
                        &decoded_frame,
                        frame_count,
                        cfg,
                        stream_url,
                        uuid,
                        stream_md5_val,
                        project_uuid,
                        organization_uuid,
                        pts * timebase_num / timebase_den,
                    ) {
                        Ok(_) => {}
                        Err(e) => {
                            println!("处理帧错误: {e:?}");
                        }
                    }

                    // 每30帧打印一次
                    if cfg.is_test {
                        println!(
                            "已处理 {} 帧 | 帧率: {:.2} FPS",
                            frame_count,
                            frame_count as f64 / start_time.elapsed().as_secs_f64()
                        );
                    }
                    // 冲洗解码器
                    v_dctx.flush();
                }
            }
        }

        // 避免无限运行下去
        if start_time.elapsed().as_secs_f64() >= cfg.max_duration {
            break;
        }
    }

    let elapsed = start_time.elapsed().as_secs_f64();
    if cfg.is_test {
        println!(
            "处理完成: 在 {:.2} 秒内处理了 {} 帧 | 平均帧率: {:.2} FPS",
            elapsed,
            frame_count,
            frame_count as f64 / elapsed
        );
    }

    Ok(())
}

fn process_frame(
    scaler: &mut scaling::Context,
    frame: &frame::Video,
    frame_count: u128,
    cfg: &Config,
    stream_url: &str,
    uuid: &str,
    stream_md5_val: &str,
    project_uuid: &str,
    organization_uuid: &str,
    pts: i64, // 当前帧是播放到了第几秒
) -> Result<()> {
    // 获取帧的基本信息
    let width = frame.width();
    let height = frame.height();

    let mut rgb_frame = frame::Video::empty();
    // let mut rgb_frame = frame::Video::new(format::Pixel::RGB8, width, height);
    scaler
        .run(frame, &mut rgb_frame)
        .context("帧格式转换失败")?;

    let mut image_buffer = ImageBuffer::<Rgb<u8>, Vec<u8>>::new(width, height);

    // 复制像素数据到图像缓冲区
    let data = rgb_frame.data(0);
    let stride = rgb_frame.stride(0);

    for y in 0..height {
        for x in 0..width {
            let offset = (y as usize * stride) + (x as usize * 3);
            let r = data[offset];
            let g = data[offset + 1];
            let b = data[offset + 2];
            image_buffer.put_pixel(x, y, Rgb([r, g, b]));
        }
    }

    let pf = PathBuf::from(&cfg.dump_path).join(format!(
        "{}_{}_{}.jpg",
        stream_md5_val,
        get_current_str(Some("")),
        frame_count
    ));
    let file_path = pf.display().to_string();
    image_buffer
        .save(&pf)
        .context(format!("保存图像失败: {:?}", &file_path))?;

    let _ = to_predict_db(
        cfg,
        pf.to_str()
            .ok_or_else(|| anyhow!("pathbuf can not conver to string"))?,
        stream_url,
        uuid,
        project_uuid,
        organization_uuid,
        stream_md5_val,
        pts,
    );

    Ok(())
}

fn to_predict_db(
    cfg: &Config,
    pf_str: &str, // from PathBuf to String using to_string_lossy
    stream_url: &str,
    uuid: &str,
    project_uuid: &str,
    organization_uuid: &str,
    stream_md5_val: &str,
    pts: i64,
) -> Result<()> {
    let conn = Connection::open(&cfg.db_path)?;
    let sql = r#"
        insert into pic(
            path, stream_url, uuid,
            project_uuid, organization_uuid,
            stream_md5, pts
        ) values(?,?,?,?,?,?,?)
    "#;
    let _ = conn.execute(
        sql,
        (
            pf_str,
            stream_url,
            uuid,
            project_uuid,
            organization_uuid,
            stream_md5_val,
            pts,
        ),
    )?;

    Ok(())
}
