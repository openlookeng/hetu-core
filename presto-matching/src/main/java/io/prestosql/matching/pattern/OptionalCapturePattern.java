/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.prestosql.matching.pattern;

import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Match;
import io.prestosql.matching.Pattern;
import io.prestosql.matching.PatternVisitor;

import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * This {@link Pattern} is very similar to the {@link io.prestosql.matching.pattern.CapturePattern}
 * but it captures only the objects that satisfy the given predicate. If the object satisfies the
 * predicate, the captured object will be an Optional of that object. If not,
 * this class will capture an Optional.empty() reference indicating that the condition didn't match.
 * <p>
 * This pattern is intended to be used with the Patterns.optionalSource method as shown in the
 * following example:
 * <pre>
 *     aggregation().with(optionalSource(ProjectNode.class)
 *                 .matching(anyPlan().capturedAsIf(x -> x instanceof ProjectNode, OPTIONAL_POST_PROJECT)
 *                         .with(source().matching(
 *                                 tableScan().capturedAs(TABLE_SCAN)))
 * </pre>
 *
 * @param <T> the Pattern type
 */
public class OptionalCapturePattern<T>
        extends Pattern<T>
{
    private final Predicate<T> predicate;

    private final Capture<Optional<T>> capture;

    public OptionalCapturePattern(Predicate<T> predicate, Capture<Optional<T>> capture, Pattern<T> previous)
    {
        super(previous);
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.capture = requireNonNull(capture, "capture is null");
    }

    public Capture<Optional<T>> capture()
    {
        return capture;
    }

    @Override
    public <C> Stream<Match> accept(Object object, Captures captures, C context)
    {
        Captures newCaptures;
        if (this.predicate.test((T) object)) {
            newCaptures = captures.addAll(Captures.ofNullable(capture, Optional.of((T) object)));
        }
        else {
            newCaptures = captures.addAll(Captures.ofNullable(capture, Optional.empty()));
        }
        return Stream.of(Match.of(newCaptures));
    }

    @Override
    public void accept(PatternVisitor patternVisitor)
    {
        patternVisitor.visitOptionalCapture(this);
    }
}
