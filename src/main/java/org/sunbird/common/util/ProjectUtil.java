package org.sunbird.common.util;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;
import org.apache.poi.ss.usermodel.*;
import org.joda.time.DateTime;
import org.springframework.http.HttpStatus;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.common.exceptions.ProjectCommonException;
import org.sunbird.common.exceptions.ResponseCode;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.model.SunbirdApiRespParam;
import org.sunbird.core.logger.CbExtLogger;

/**
 * This class will contains all the common utility methods.
 *
 * @author Manzarul
 */
public class ProjectUtil {

	public static CbExtLogger logger = new CbExtLogger(ProjectUtil.class.getName());

	public static String DEFAULT_BULK_UPLOAD_VERIFICATION_REGEX ="^(?!.*\\n)[a-zA-Z\\s,]+$";

	/**
	 * This method will check incoming value is null or empty it will do empty check
	 * by doing trim method. in case of null or empty it will return true else
	 * false.
	 *
	 * @param value
	 * @return
	 */
	public static boolean isStringNullOREmpty(String value) {
		return (value == null || "".equals(value.trim()));
	}

	/**
	 * This method will create and return server exception to caller.
	 *
	 * @param responseCode ResponseCode
	 * @return ProjectCommonException
	 */
	public static ProjectCommonException createServerError(ResponseCode responseCode) {
		return new ProjectCommonException(responseCode.getErrorCode(), responseCode.getErrorMessage(),
				ResponseCode.SERVER_ERROR.getResponseCode());
	}

	public static ProjectCommonException createClientException(ResponseCode responseCode) {
		return new ProjectCommonException(responseCode.getErrorCode(), responseCode.getErrorMessage(),
				ResponseCode.CLIENT_ERROR.getResponseCode());
	}

	public static SBApiResponse createDefaultResponse(String api) {
		SBApiResponse response = new SBApiResponse();
		response.setId(api);
		response.setVer(Constants.API_VERSION_1);
		response.setParams(new SunbirdApiRespParam(UUID.randomUUID().toString()));
		response.getParams().setStatus(Constants.SUCCESS);
		response.setResponseCode(HttpStatus.OK);
		response.setTs(DateTime.now().toString());
		return response;
	}

	public static Map<String, String> getDefaultHeaders() {
		Map<String, String> headers = new HashMap<String, String>();
		headers.put(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON);
		return headers;
	}

	public enum Method {
		GET, POST, PUT, DELETE, PATCH
	}

	public static String convertSecondsToHrsAndMinutes(int seconds) {
		String time = "";
		if (seconds > 60) {
			int min = (seconds / 60) % 60;
			int hours = (seconds / 60) / 60;
			String minutes = (min < 10) ? "0" + min : Integer.toString(min);
			String strHours = (hours < 10) ? "0" + hours : Integer.toString(hours);
			if (min > 0 && hours > 0)
				time = strHours + "h " + minutes + "m";
			else if (min == 0 && hours > 0)
				time = strHours + "h";
			else if (min > 0) {
				time = minutes + "m";
			}
		}
		return time;
	}

	public static String firstLetterCapitalWithSingleSpace(final String words) {
		return Stream.of(words.trim().split("\\s")).filter(word -> word.length() > 0)
				.map(word -> word.substring(0, 1).toUpperCase() + word.substring(1)).collect(Collectors.joining(" "));
	}
	/**
	 * Check the email id is valid or not
	 *
	 * @param email String
	 * @return Boolean
	 */

	public static Boolean validateEmailPattern(String email) {
		String emailRegex = "^[a-zA-Z0-9_+&*-]+(?:\\." + "[a-zA-Z0-9_+&*-]+)*@" + "(?:[a-zA-Z0-9-]+\\.)+[a-z"
				+ "A-Z]{2,7}$";
		Pattern pat = Pattern.compile(emailRegex);
		if (pat.matcher(email).matches()) {
			return Boolean.TRUE;
		}
		return Boolean.FALSE;
	}

	/**
	 * Check the contact number is valid or not
	 *
	 * @param contactNumber String
	 * @return Boolean
	 */

	public static Boolean validateContactPattern(String contactNumber) {
		String contactNumberRegex = "^\\d{10}$";
		Pattern pat = Pattern.compile(contactNumberRegex);
		if (pat.matcher(contactNumber).matches()) {
			return Boolean.TRUE;
		}
		return Boolean.FALSE;
	}

	public static Boolean validateFullName(String firstName) {
		return firstName.matches("^(?!.*\\n)[a-zA-Z]+(?:['\\s][a-zA-Z]+)*(?<!\\.|\\s)$");
	}

	public static Boolean validateTag(List<String> tags) {
		String regEx = PropertiesCache.getInstance().getProperty(Constants.BULK_UPLOAD_VERIFICATION_REGEX);
		if (StringUtils.isBlank(regEx)) {
			regEx = DEFAULT_BULK_UPLOAD_VERIFICATION_REGEX;
		}
		for (String tag : tags) {
			if (!tag.matches(regEx)) {
				return false;
			}
		}
		return true;
	}

	public static Boolean validateExternalSystemId(String externalSystemId) {
		return externalSystemId.matches("^(?=.{1,30}$)[a-zA-Z0-9]+(?:-[a-zA-Z0-9]+)*$");
	}

	public static Boolean validateExternalSystem(String externalSystem) {
		return externalSystem.matches("^(?=.*[a-zA-Z .-])[a-zA-Z0-9 .-]{1,255}$"); // Allow only alphanumeric, alphabets and restrict if only numeric character
	}

	public static void updateErrorDetails(SBApiResponse response, String errMsg, HttpStatus responseCode) {
		response.getParams().setStatus(Constants.FAILED);
		response.getParams().setErrmsg(errMsg);
		response.setResponseCode(responseCode);

	}

	public static SimpleDateFormat getDateFormatter() {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSSZ");
		simpleDateFormat.setLenient(false);
		return simpleDateFormat;
	}

	public static String getFormattedDate() {
		return getDateFormatter().format(new Date());
	}

	public static String decodeUrl(String encodeString){
        try {
            return URLDecoder.decode(encodeString, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

	public static Boolean validateDate(String dateString){
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
		dateFormat.setLenient(false);
		try {
			Date todaysDate = new Date();
			Calendar calendar = Calendar.getInstance();
			calendar.add(Calendar.YEAR, -65);
			Date pastDate = calendar.getTime();
			Date date = dateFormat.parse(dateString);
			return date.after(pastDate) && (date.before(todaysDate) || date.equals(todaysDate));
		} catch (ParseException e) {
			logger.error("Invalid date format: " + dateString + " Exception: ", e);
		}
		return false;
	}

	public static Boolean validateEmployeeId(String employeeId) {
		return employeeId.matches("^[a-zA-Z0-9]{1,30}$"); // Allow only alphanumeric,numeric and alphabetic.
	}

	public static Boolean validateRegexPatternWithNoSpecialCharacter(String regex) {
		return regex.matches("^[a-zA-Z0-9 \\-()]*$");
	}

	public static Boolean validateRegexPatternWithSpecialCharacter(String regex) {
		return regex.matches("^[a-zA-Z0-9 \\-()&/,+]*$");
	}

	public static Boolean validatePinCode(String regex) {
		return regex.matches("^[0-9]{6}$");
	}

	public static Boolean validatesNewLine(String value) {
		return value.matches(".*\\n.*");
	}

	public static boolean hasValidRowCountInXLSFile(MultipartFile file, int maximumAllowedLimit) {
		int validRowCount = 0;
		try (InputStream inputStream = file.getInputStream(); Workbook workbook = WorkbookFactory.create(inputStream)) {
			Sheet sheet = workbook.getSheetAt(0);
			Iterator<Row> rowIterator = sheet.iterator();
			// Skip header
			if (rowIterator.hasNext()) {
				rowIterator.next();
			}

			while (rowIterator.hasNext()) {
				Row row = rowIterator.next();
				boolean isFirstColumnNotEmpty = false;

				Cell cell = row.getCell(0); // Check only first column
				if (cell != null && !(cell.getCellType() == CellType.BLANK ||
						(cell.getCellType() == CellType.STRING && cell.getStringCellValue().trim().isEmpty()))) {
					isFirstColumnNotEmpty = true;
				}
				if (isFirstColumnNotEmpty) {
					validRowCount++;
				}
				if (validRowCount > maximumAllowedLimit) {
					return false;
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("Failed to validate Excel file", e);
		}
		return true;
	}

	public static Map<String, String> getDefaultHeadrs(String userAuthToken) {
		Map<String, String> headers = new HashMap<String, String>();
		headers.put(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON);
		headers.put(Constants.X_AUTH_TOKEN, userAuthToken);
		return headers;
	}

    public static SBApiResponse returnErrorMsg(String errMsg, HttpStatus httpStatus, SBApiResponse response, String status) {
        response.getParams().setStatus(status);
        response.getParams().setErrmsg(errMsg);
        response.setResponseCode(httpStatus);
        response.getResult().put("message", errMsg);
        return response;
    }

	public static enum ESIndexType {
		SUNBIRD_ES("sunbird_es"), USER_ES("user_es"), IGOT_ES("igot_es");
		private String indexName;

		ESIndexType(String indexName) {
			this.indexName = indexName;
		}

		public String getIndexName() {
			return indexName;
		}
	}
}
