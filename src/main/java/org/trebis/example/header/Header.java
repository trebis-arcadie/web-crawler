package org.trebis.example.header;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class Header {
    private String userAgent;
    private String acceptLanguage;
    private String acceptEncoding;
    private String accept;
    private String referer;
}
