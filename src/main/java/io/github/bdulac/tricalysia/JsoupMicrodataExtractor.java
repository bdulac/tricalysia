package io.github.bdulac.tricalysia;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.Triple;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Attribute;
import org.jsoup.nodes.Attributes;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class JsoupMicrodataExtractor implements TriplesExtractor {
	
	public JsoupMicrodataExtractor() {
	}
	
	@Override
	public List<String> getSupportedMimeTypes() {
		List<String> result = new ArrayList<String>();
		result.add("text/html");
		result.add("text/xhtml");
		return result;
	}
	
	@Override
	public List<URL> extract(Tricalysia store, URL url) throws IOException {
		List<URL> result = new ArrayList<URL>();
		Document doc = Jsoup.connect(url.toString()).get();
		Elements htmlDocs = doc.select("html");
		for(Element htmlDoc : htmlDocs) {
			Element root = htmlDoc;
			processDocument(root, store, url, result);
		}
		Elements links = doc.getElementsByAttribute("href");
		for(Element linkEl : links) {
				String href = linkEl.attr("href");
				String rel = linkEl.attr("rel");
				if(
						(href != null) 
						&& (href.trim().length() > 0)
						&& (!"stylesheet".equalsIgnoreCase(rel))
						&& (!"apple-touch-icon".equalsIgnoreCase(rel))
						&& (!"shortcut-icon".equalsIgnoreCase(rel))
						&& (!"alternate".equalsIgnoreCase(rel))
						&& (!"import".equalsIgnoreCase(rel))
						&& (!"canonical".equalsIgnoreCase(rel))
				) {
					
					URL link = null;
					try {
						link = URLResolver.resolveRelativeURL(href,url);
						if(link.equals(url)) {
							continue;
						}
						result.add(link);
					} catch(MalformedURLException e) {
						Logger.getAnonymousLogger().warning(e.getMessage());
					}
				}
		}
		return result;
	}

	private void processDocument(
			Element root, 
			Tricalysia store, 
			URL url, 
			List<URL> result
	) {
		List<Triple<String, String, String>> triples = 
				new ArrayList<Triple<String, String, String>>();
		if(url != null) {
			fetchTriplesFromDom(root, triples, false);

		}
		for(Triple<String, String, String> triple : triples) {
			try {
				store.write(
						triple.getLeft(), 
						triple.getMiddle(), 
						triple.getRight()
				);
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
			/*
			fillResultFromUriString(
					triple.getLeft(), 
					url, 
					result
			);
			fillResultFromUriString(
					triple.getMiddle(), 
					url, 
					result
			);
			fillResultFromUriString(
					triple.getRight(), 
					url, 
					result
			);
			*/
		}
	}
	
	private String fetchTriplesFromDom(
			Element element, 
			List<Triple<String, String, String>> result, 
			boolean registerHtmlTriples
	) {
		String currentIdStr = fetchSubjectId(element);
		
		Attributes attrs = element.attributes();
		for(Attribute attr : attrs) {
			if(registerHtmlTriples) {
				result.add(
						Triple.of(
								currentIdStr, 
								"hasHtmlAttr[" + attr.getKey() + "]", 
								attr.getValue())
				);
			}
			if(attr.getKey().equalsIgnoreCase("itemprop")) {
				// On a property, the scope should not be on the local element
				final String scopeId = getScopeId(element.parent());
				if(attr.getValue().equalsIgnoreCase("sameas")) {
					String href = fetchAttr(element, "href");
					if((href != null) && (href.trim().length() > 0)) {
						result.add(
								Triple.of(
										scopeId, attr.getValue(), href
								)
						);
					}
					else {
						result.add(
								Triple.of(
										scopeId, attr.getValue(), currentIdStr
								)
						);
					}
				}
				else {
					result.add(
							Triple.of(
									scopeId, attr.getValue(), currentIdStr
							)
					);
				}
			}
			if(attr.getKey().equalsIgnoreCase("itemtype")) {
				final String scopeId = getScopeId(element);
				result.add(Triple.of(scopeId, attr.getKey(), attr.getValue()));
			}
		}
		for(Element child : element.children()) {
			String childIdStr = fetchSubjectId(child);
			if(registerHtmlTriples) {
				result.add(Triple.of(childIdStr, "isDOMChildOf", currentIdStr));
			}
			fetchTriplesFromDom(child, result, registerHtmlTriples);
		}
		if(registerHtmlTriples) {
			result.add(Triple.of(currentIdStr, "isHtmlElement", element.tagName()));
		}
		return currentIdStr;
	}
	
	private static String getScopeId(Element element) {
		String scopeId = null;
		// Trying to get a scope ID from a direct scope element
		if(element.hasAttr("itemscope")) {
			scopeId = fetchSubjectId(element);
		}
		// If no scope ID from the element,
		if(scopeId == null) {
			// Then, looking for an "itemscope" attibute in the parents		
			Element el = element;
			while(el.parent() != null && scopeId == null) {
				if(el.parent().hasAttr("itemscope")) {
					scopeId = fetchSubjectId(el.parent());
				}
				el = el.parent();
			}
			// In the end, we might get the root
			if(scopeId == null) {
				scopeId = fetchSubjectId(el);
			}
		}
		return scopeId;
	}

	private static String fetchSubjectId(Element item) {
		String subject = null;
		String id = item.id();
		// If there is no id, 
		if((id == null) || (id.trim().length()== 0)) {
			// If we have a body
			if(item.tagName().equalsIgnoreCase("body")) {
				// Then we will refer to the document
				while(item.parent() != null) {
					item = item.parent();
				}
			}
			// If the item is the document or the HTML element
			if(
					(item.tagName().equalsIgnoreCase("html") 
					|| item.parent() == null)
			) {
				// Then the subject is the title or a canonical link
				Elements titles = item.getElementsByTag("title");
				for(Element title : titles) {
					if(
							title.hasText() 
							&& (title.text().trim().length() > 0)
					) {
						subject = title.text();
						break;
					}
				}
				if(subject == null) {
					Elements canonicals = 
							item.getElementsByAttributeValue(
									"rel", 
									"canonical"
							);
					for(Element canonical : canonicals) {
						subject = fetchAttr(canonical, "href");
					}
				}
			}
			// If the item has a text content
			else if(
					item.hasText() 
					&& (item.children().size() == 0) 
					&& item.text().trim().length() > 0
			) {
				// Then this is goint to be the subject
				subject = item.text();
			}
			// If the item has a content attribute
			else if(item.hasAttr("content")) {
				subject = fetchAttr(item, "content");
			}
			else {
				// Else we look for a representative property on a child
				for(Element child : item.children()) {
					// name
					if(
							(child.attr("itemprop").equalsIgnoreCase("name")) 
							&& child.hasText()
					) {
						subject = child.text();
					}
					// sameAs
					else if(
							(
									child.attr("itemprop")
									.equalsIgnoreCase("sameas"))
					) {
						subject = fetchAttr(child, "href");
						if(subject == null) {
							if(child.hasText()) {
								subject = child.text();
							}
						}
					}
					if(subject != null) {
						break;
					}
				}
			}
		}
		else {
			// If there is an ID
			subject = '#' + id;
		}
		if((subject == null) || (subject.trim().length() == 0)) {
			subject = item.tagName() + "#" + item.hashCode();
		}
		return subject;
	}

	private static String fetchAttr(Element element, String attr) {
		if(element == null)return null;
		String attrValue = null;
		if(element.hasAttr(attr)) {
			attrValue = element.attr(attr);
			if(attrValue == null || attrValue.trim().length() == 0) {
				Elements attrs = element.getElementsByAttribute(attr);
				for(Element attrEl : attrs) {
					if((attrEl.hasText()) && (attrEl.text().trim().length() > 0)) {
						attrValue = attrEl.text();
					}
				}
			}
		}
		return attrValue;
	}
}