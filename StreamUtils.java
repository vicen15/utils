package es.gob.ad.servicios.horizontales.trama.migracion.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.slf4j.Logger;

/**
 * Utilidad para filtrar streams y registrar trazas sin usar 
 */
public class StreamUtils {

    /**
     * Inicia el builder de filtrado con logs.
     */
    public static <T> StreamBuilder<T> filterWithLogs(Stream<T> stream, Predicate<T> predicate) {
        return new StreamBuilder<>(stream, predicate);
    }

    // =====================================================================================
    // FIELD WRAPPER
    // =====================================================================================

    @FunctionalInterface
    public interface Field<T> {
        Object apply(T item);
    }

    public static <T> Field<T> field(Function<T, Object> getter) {
        return getter::apply;
    }

    // =====================================================================================
    // LOG RULE
    // =====================================================================================

    public static class LogRule<T> {
        final Predicate<T> condition;
        final String message;
        final Field<T>[] fields;

        @SafeVarargs
        public LogRule(Predicate<T> condition, String message, Field<T>... fields) {
            this.condition = condition;
            this.message = message;
            this.fields = fields;
        }

        public Object[] extractValues(T item) {
            Object[] values = new Object[fields.length];
            for (int i = 0; i < fields.length; i++) {
                values[i] = fields[i].apply(item);
            }
            return values;
        }
    }

    // =====================================================================================
    // BUILDER
    // =====================================================================================

    public static class StreamBuilder<T> {

        private final Stream<T> source;
        private final Predicate<T> predicate;
        private final List<LogRule<T>> rules = new ArrayList<>();


        StreamBuilder(Stream<T> source, Predicate<T> predicate) {
            this.source = source;
            this.predicate = predicate;
        }

        @SafeVarargs
        public final StreamBuilder<T> logWhen(Predicate<T> condition, String message, Field<T>... fields) {
            this.rules.add(new LogRule<>(condition, message, fields));
            return this;
        }

        private void ensureLoggerPresent(Logger logger) {
            if (logger == null) {
                throw new IllegalStateException(
                    "logger == null en la llamada a build(Logger logger)."
                );
            }
        }

        /**
         * Construye el stream filtrado que registra trazas de forma segura con paralelismo.
         */
        public Stream<T> build(Logger logger) {
            ensureLoggerPresent(logger);

            return source.filter(item -> {

                // ¿El elemento debe ser filtrado?
                boolean toFilter = predicate.test(item);

				// Ejecutar reglas de log
				for (LogRule<T> rule : rules) {
					if (rule.condition.test(item)) {
						logger.info(rule.message, rule.extractValues(item));
					}
				}

                return toFilter;
            });
        }
    }
}
