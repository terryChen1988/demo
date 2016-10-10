package frame_test;

import com.helijia.framework.retry.RetryCallback;
import com.helijia.framework.retry.RetryContext;
import com.helijia.framework.retry.policy.SimpleRetryPolicy;
import com.helijia.framework.retry.support.RetryTemplate;

public class RetryTest {

	public static void main(String[] args) {
		RetryTemplate retryTemplate = new RetryTemplate();

		// 默认最多重试3次
		try {
			retryTemplate.execute(new RetryCallback<String>() {
				@Override
				public String retry(RetryContext context) throws Exception {
					System.out.println("hello");
					
					if (true) {
						throw new RuntimeException("aaa");
					}
					
					return "Retry OK";
				}
			}, new SimpleRetryPolicy(3));
		} catch (Exception e) {

		}
	}
}
