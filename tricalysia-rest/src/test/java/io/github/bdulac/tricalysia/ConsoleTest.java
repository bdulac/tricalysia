package io.github.bdulac.tricalysia;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


public class ConsoleTest {
	
	private Console subject;
	
	private Thread th;
	
	@BeforeEach
	public void setUp() {
		subject = new Console(9980);
		th = new Thread(subject);
		th.setDaemon(false);
		th.start();
	}
	
	@AfterEach
	public void tearDown() {
		if(subject != null) {
			subject.stop();
			subject = null;
			th = null;
		}
	}
	
	@Test
	public void testIsDeamon() {
		Assertions.assertFalse(th.isDaemon());
		Assertions.assertTrue(th.isAlive());
	}

	/*
	@Test
	@Ignore
	public void testMain() {
		if(subject != null) {
			subject.stop();
			subject = null;
		}
		Console.main(new String[] {"9980"});
	}
	*/
	
}
