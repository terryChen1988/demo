package netty_test.server;

import org.apache.log4j.Logger;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class NettyServerBootstrap {

	private static Logger logger = Logger.getLogger(NettyServerBootstrap.class);

	private int port;

	public NettyServerBootstrap(int port) {
		this.port = port;
		bind();
	}

	private void bind() {

		EventLoopGroup boss = new NioEventLoopGroup();
		EventLoopGroup worker = new NioEventLoopGroup();

		try {
			ServerBootstrap bootstrap = new ServerBootstrap();

			bootstrap.group(boss, worker);
			bootstrap.channel(NioServerSocketChannel.class);
			bootstrap.option(ChannelOption.SO_BACKLOG, 1024); // 连接数
			bootstrap.option(ChannelOption.TCP_NODELAY, true); // 不延迟，消息立即发送
			bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true); // 长连接
			bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
				@Override
				protected void initChannel(SocketChannel socketChannel) throws Exception {
					ChannelPipeline p = socketChannel.pipeline();
					p.addLast(new NettyServerInitializer());
				}
			});
			ChannelFuture f = bootstrap.bind(port).sync();
			if (f.isSuccess()) {
				logger.info("启动Netty服务成功，端口号：" + this.port);
			}
			// 关闭连接
			f.channel().closeFuture().sync();

		} catch (Exception e) {
			logger.error("启动Netty服务异常，异常信息：" + e.getMessage());
			e.printStackTrace();
		} finally {
			boss.shutdownGracefully();
			worker.shutdownGracefully();
		}
	}

	public static void main(String[] args) throws InterruptedException {

		NettyServerBootstrap server = new NettyServerBootstrap(9999);

	}

}