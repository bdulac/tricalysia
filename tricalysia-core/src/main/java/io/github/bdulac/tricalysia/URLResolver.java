package io.github.bdulac.tricalysia;

import java.net.MalformedURLException;
import java.net.URL;

public final class URLResolver {
	
	public static URL resolveRelativeURL(String url, URL refUrl) 
			throws MalformedURLException {
		if(url == null) {
			return null;
		}
		if(url.endsWith("#")) {
			url = url.substring(0, url.length() - 1);
		}
		String result = url;
		if(
				(url != null)
				&& (!url.equals(refUrl.toString()))
				&& (!url.equals(refUrl.toString() + "#"))
				&& (!url.startsWith("http://"))
		) {
			String str = refUrl.toString();
			if(!str.endsWith("/")) {
				if(url.startsWith("/")) {
					str = str.substring(0, str.substring(8).indexOf("/") + 8);
				}
				else {
					str = str.substring(0, str.lastIndexOf("/") + 1);
				}
				result = str + url;
			}
			else {
				result = str + "/" + url;
			}
		}
		return new URL(result);
	}

}
